"""A leaderless Dynamo-style replicated key-value node.

Every Delivery Status pod runs one `DynamoNode`. Any node can *coordinate* any
read or write - there is no leader and no single point of failure.

  * Partitioning   : consistent hashing with virtual nodes (ring.py)
  * Replication    : each key on N nodes = its preference list
  * Tunable quorum : write succeeds after W acks, read after R replies (R+W>N)
  * Versioning     : vector clocks; concurrent writes are kept as siblings
  * Sloppy quorum  : if a preference-list node is down, the next healthy node on
                     the ring stores the write with a *hint* for the real owner
  * Hinted handoff : hints are pushed back once the owner is reachable again
  * Read repair    : stale replicas seen during a read are updated
  * Anti-entropy   : background Merkle-tree comparison per peer repairs
                     replicas that missed writes (e.g. after a crash)
  * Failure detect : periodic gRPC pings; unreachable peers are marked down
"""
import hashlib
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import grpc
from prometheus_client import Counter, Gauge, Histogram

import dynamo_pb2 as pb
import dynamo_pb2_grpc as pb_grpc

from . import vclock
from .ring import Ring, h
from .storage import Store

log = logging.getLogger("dynamo")

WRITES = Counter("dynamo_quorum_writes_total", "Coordinated writes", ["result"])
READS = Counter("dynamo_quorum_reads_total", "Coordinated reads", ["result"])
WRITE_LAT = Histogram("dynamo_write_latency_seconds", "Quorum write latency",
                      buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2))
READ_LAT = Histogram("dynamo_read_latency_seconds", "Quorum read latency",
                     buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2))
HINTS_STORED = Counter("dynamo_hints_stored_total", "Hinted writes accepted for a down replica")
HINTS_DELIVERED = Counter("dynamo_hints_delivered_total", "Hints handed back to their owner")
HINTS_PENDING = Gauge("dynamo_hints_pending", "Hints waiting for delivery")
READ_REPAIRS = Counter("dynamo_read_repairs_total", "Replicas repaired during reads")
SIBLINGS = Counter("dynamo_siblings_detected_total", "Reads that found concurrent siblings")
AE_SYNCED = Counter("dynamo_antientropy_keys_synced_total", "Keys repaired by Merkle anti-entropy")
AE_ROUNDS = Counter("dynamo_antientropy_rounds_total", "Anti-entropy exchanges", ["result"])
PEER_UP = Gauge("dynamo_peer_up", "Failure detector view of a peer (1=up)", ["peer"])
KEYS = Gauge("dynamo_keys", "Keys stored locally")
SIM_FAILED = Gauge("dynamo_simulated_failure", "1 while this node simulates a crash")


class QuorumError(Exception):
    pass


@dataclass
class ReadResult:
    versions: list[dict]
    responses: dict[str, list[dict] | None]
    siblings: int
    repaired: list[str] = field(default_factory=list)
    preference_list: list[str] = field(default_factory=list)


@dataclass
class WriteResult:
    acks: list[str]
    hinted: dict[str, str]
    preference_list: list[str]


def _bucket(khash: int, buckets: int) -> int:
    return khash % buckets


class DynamoNode:
    def __init__(self, node_id: str, peers: dict[str, str], store: Store,
                 n: int = 3, r: int = 2, w: int = 2, vnodes: int = 64, buckets: int = 64,
                 rpc_timeout: float = 0.8):
        self.node_id = node_id
        self.peers = peers                       # node_id -> "host:port" (includes self)
        self.store = store
        self.n, self.r, self.w = min(n, len(peers)), r, w
        self.buckets = buckets
        self.rpc_timeout = rpc_timeout
        self.ring = Ring(list(peers), vnodes)
        self.health = {p: {"up": True, "fails": 0, "last_seen": None} for p in peers if p != node_id}
        self.failed_until = 0.0
        self._stubs: dict[str, pb_grpc.DynamoNodeStub] = {}
        self._pool = ThreadPoolExecutor(max_workers=32, thread_name_prefix="dynamo-rpc")
        self._stop = threading.Event()
        self.stats = {"hints_delivered": 0, "read_repairs": 0, "ae_keys_synced": 0, "last_ae": None}

    # ================================================================ membership
    def is_failed(self) -> bool:
        return time.time() < self.failed_until

    def simulate_failure(self, seconds: float) -> None:
        self.failed_until = time.time() + seconds
        SIM_FAILED.set(1)
        log.warning("SIMULATED NODE FAILURE", extra={"node": self.node_id, "seconds": seconds})

    def recover(self) -> None:
        self.failed_until = 0
        SIM_FAILED.set(0)

    def is_up(self, node: str) -> bool:
        if node == self.node_id:
            return not self.is_failed()
        return self.health[node]["up"]

    def _mark(self, node: str, ok: bool) -> None:
        if node == self.node_id:
            return
        hstate = self.health[node]
        if ok:
            if not hstate["up"]:
                log.info("peer is back up", extra={"peer": node})
            hstate.update(up=True, fails=0, last_seen=time.time())
        else:
            hstate["fails"] += 1
            if hstate["up"] and hstate["fails"] >= 1:
                log.warning("peer marked down", extra={"peer": node})
                hstate["up"] = False
        PEER_UP.labels(node).set(1 if hstate["up"] else 0)

    def _stub(self, node: str) -> pb_grpc.DynamoNodeStub:
        if node not in self._stubs:
            channel = grpc.insecure_channel(self.peers[node], options=[
                ("grpc.keepalive_time_ms", 10000), ("grpc.max_receive_message_length", 64 * 1024 * 1024)])
            self._stubs[node] = pb_grpc.DynamoNodeStub(channel)
        return self._stubs[node]

    # ================================================================ replica-side
    def local_put(self, key: str, versions: list[dict], hint_for: str = "") -> bool:
        if hint_for and hint_for != self.node_id:
            self.store.add_hint(hint_for, key, versions)
            HINTS_STORED.inc()
            return True
        self.store.merge_put(key, versions)
        return True

    def local_get(self, key: str) -> list[dict] | None:
        return self.store.get(key)

    def shared_digests(self, peer: str) -> list[tuple[str, str, int]]:
        """Keys this node holds that `peer` should also hold (both in the preference list)."""
        out = []
        for key, dig, khash in self.store.digests():
            pref = self.ring.preference_list(key, self.n)
            if peer in pref and self.node_id in pref:
                out.append((key, dig, khash))
        return out

    def merkle(self, peer: str) -> tuple[str, list[str]]:
        leaves = [hashlib.sha256() for _ in range(self.buckets)]
        for key, dig, khash in sorted(self.shared_digests(peer)):
            leaves[_bucket(khash, self.buckets)].update(f"{key}={dig};".encode())
        leaf_hex = [l.hexdigest()[:16] for l in leaves]
        # Binary Merkle tree over the leaves; the root summarises everything.
        level = leaf_hex
        while len(level) > 1:
            level = [hashlib.sha256((level[i] + (level[i + 1] if i + 1 < len(level) else "")).encode())
                     .hexdigest()[:16] for i in range(0, len(level), 2)]
        return level[0], leaf_hex

    def bucket_entries(self, peer: str, bucket: int) -> dict[str, list[dict]]:
        return {key: self.store.get(key) for key, _, khash in self.shared_digests(peer)
                if _bucket(khash, self.buckets) == bucket}

    # ================================================================ RPC helpers
    def rpc_put(self, node: str, key: str, versions: list[dict], hint_for: str = "") -> bool:
        if node == self.node_id:
            if self.is_failed():
                return False
            return self.local_put(key, versions, hint_for)
        try:
            reply = self._stub(node).ReplicaPut(pb.PutRequest(
                key=key, versions_json=json.dumps(versions), hint_for=hint_for, coordinator=self.node_id),
                timeout=self.rpc_timeout)
            self._mark(node, True)
            return reply.ok
        except grpc.RpcError:
            self._mark(node, False)
            return False

    def rpc_get(self, node: str, key: str) -> tuple[bool, list[dict] | None]:
        """Returns (responded, versions)."""
        if node == self.node_id:
            if self.is_failed():
                return False, None
            return True, self.local_get(key)
        try:
            reply = self._stub(node).ReplicaGet(pb.GetRequest(key=key), timeout=self.rpc_timeout)
            self._mark(node, True)
            return True, (json.loads(reply.versions_json) if reply.found else None)
        except grpc.RpcError:
            self._mark(node, False)
            return False, None

    # ================================================================ coordinator
    def _targets(self, key: str) -> tuple[list[str], list[tuple[str, str]], list[str]]:
        walk = list(self.ring.walk(key))
        natural = walk[: self.n]
        spares = [s for s in walk[self.n:]]
        targets, used = [], set()
        for node in natural:
            if self.is_up(node):
                targets.append((node, ""))
                used.add(node)
        for node in natural:
            if not self.is_up(node):
                sub = next((s for s in spares if s not in used and self.is_up(s)), None)
                if sub:
                    targets.append((sub, node))   # sloppy quorum: sub holds a hint for `node`
                    used.add(sub)
        return natural, targets, spares

    def coordinate_put(self, key: str, versions: list[dict], w: int | None = None) -> WriteResult:
        w = w or self.w
        start = time.perf_counter()
        natural, targets, spares = self._targets(key)
        acks, hinted, failed = [], {}, []
        futs = {self._pool.submit(self.rpc_put, node, key, versions, hint): (node, hint) for node, hint in targets}
        try:
            for fut in as_completed(futs, timeout=self.rpc_timeout * 2):
                node, hint = futs[fut]
                if fut.result():
                    acks.append(node)
                    if hint:
                        hinted[node] = hint
                else:
                    failed.append(hint or node)
        except TimeoutError:
            failed.extend(hint or node for f, (node, hint) in futs.items() if not f.done())
        # Second chance: a target died between failure detection and the write.
        used = {n for n, _ in targets}
        for owner in failed:
            if len(acks) >= len(natural):
                break
            sub = next((s for s in spares if s not in used and self.is_up(s)), None)
            if sub and self.rpc_put(sub, key, versions, owner if owner != sub else ""):
                acks.append(sub)
                hinted[sub] = owner
                used.add(sub)
        WRITE_LAT.observe(time.perf_counter() - start)
        if len(acks) < w:
            WRITES.labels("quorum_failed").inc()
            raise QuorumError(f"write quorum not reached for {key}: {len(acks)}/{w} acks")
        WRITES.labels("hinted" if hinted else "ok").inc()
        return WriteResult(acks=acks, hinted=hinted, preference_list=natural)

    def coordinate_get(self, key: str, r: int | None = None) -> ReadResult:
        r = r or self.r
        start = time.perf_counter()
        walk = list(self.ring.walk(key))
        natural = walk[: self.n]
        targets = [n for n in natural if self.is_up(n)]
        responses: dict[str, list[dict] | None] = {}
        futs = {self._pool.submit(self.rpc_get, node, key): node for node in targets}
        deadline = time.time() + self.rpc_timeout * 2
        try:
            for fut in as_completed(futs, timeout=self.rpc_timeout * 2):
                responded, versions = fut.result()
                if responded:
                    responses[futs[fut]] = versions
                if len(responses) >= r:
                    # give stragglers a brief moment so read repair sees them too
                    for other in futs:
                        if other is not fut and not other.done():
                            try:
                                other.result(timeout=max(0.0, min(0.05, deadline - time.time())))
                            except Exception:
                                pass
                    for f2, node in futs.items():
                        if f2.done() and node not in responses:
                            ok, vs = f2.result()
                            if ok:
                                responses[node] = vs
                    break
        except TimeoutError:
            pass
        READ_LAT.observe(time.perf_counter() - start)
        if len(responses) < r:
            READS.labels("quorum_failed").inc()
            raise QuorumError(f"read quorum not reached for {key}: {len(responses)}/{r} replies")
        merged = vclock.reconcile([v for vs in responses.values() if vs for v in vs])
        result = ReadResult(versions=merged, responses=responses, siblings=len(merged), preference_list=natural)
        if len(merged) > 1:
            SIBLINGS.inc()
        # Read repair: push the reconciled set to replicas that returned stale data.
        if merged:
            want = vclock.digest(merged)
            for node, vs in responses.items():
                if not vs or vclock.digest(vclock.reconcile(vs)) != want:
                    result.repaired.append(node)
                    self._pool.submit(self.rpc_put, node, key, merged)
                    READ_REPAIRS.inc()
                    self.stats["read_repairs"] += 1
        READS.labels("ok").inc()
        return result

    # ================================================================ background loops
    def start(self) -> None:
        for fn, every in ((self._failure_detector, 2.0), (self._deliver_hints, 3.0), (self._anti_entropy, 20.0)):
            threading.Thread(target=self._loop, args=(fn, every), daemon=True, name=fn.__name__).start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self, fn, every: float) -> None:
        time.sleep(random.uniform(0, every))
        while not self._stop.wait(every):
            if self.is_failed():
                continue
            try:
                fn()
            except Exception as exc:
                log.warning("background task failed", extra={"task": fn.__name__, "error": str(exc)})

    def _failure_detector(self) -> None:
        KEYS.set(self.store.count())
        counts = self.store.hint_counts()
        HINTS_PENDING.set(sum(counts.values()))
        for peer in self.health:
            try:
                self._stub(peer).Ping(pb.PingRequest(from_node=self.node_id), timeout=0.5)
                self._mark(peer, True)
            except grpc.RpcError:
                self._mark(peer, False)

    def _deliver_hints(self) -> None:
        for target, count in self.store.hint_counts().items():
            if not self.is_up(target):
                continue
            delivered = 0
            for hint_id, key, versions in self.store.hints_for(target):
                if self.rpc_put(target, key, versions):
                    self.store.delete_hint(hint_id)
                    delivered += 1
                else:
                    break
            if delivered:
                HINTS_DELIVERED.inc(delivered)
                self.stats["hints_delivered"] += delivered
                log.info("hinted handoff delivered", extra={"target": target, "hints": delivered})

    def _anti_entropy(self) -> None:
        peers = [p for p in self.health if self.is_up(p)]
        if not peers:
            return
        peer = random.choice(peers)
        my_root, my_leaves = self.merkle(peer)
        try:
            reply = self._stub(peer).MerkleLeaves(pb.MerkleRequest(requester=self.node_id, buckets=self.buckets),
                                                  timeout=5)
        except grpc.RpcError:
            AE_ROUNDS.labels("peer_unreachable").inc()
            return
        self.stats["last_ae"] = {"peer": peer, "at": int(time.time() * 1000), "in_sync": reply.root == my_root}
        if reply.root == my_root:
            AE_ROUNDS.labels("in_sync").inc()
            return
        synced = 0
        for bucket, (mine, theirs) in enumerate(zip(my_leaves, reply.leaves)):
            if mine == theirs:
                continue
            entries = self.bucket_entries(peer, bucket)
            sync = self._stub(peer).SyncBucket(pb.SyncRequest(
                requester=self.node_id, bucket=bucket, buckets=self.buckets,
                entries_json=json.dumps(entries)), timeout=10)
            for key, versions in json.loads(sync.entries_json).items():
                if versions:
                    _, changed = self.store.merge_put(key, versions)
                    synced += int(changed)
        AE_ROUNDS.labels("repaired").inc()
        if synced:
            AE_SYNCED.inc(synced)
            self.stats["ae_keys_synced"] += synced
            log.info("anti-entropy repaired keys", extra={"peer": peer, "keys": synced})

    # ================================================================ introspection
    def describe(self) -> dict:
        hints = self.store.hint_counts()
        return {
            "node_id": self.node_id,
            "n": self.n, "r": self.r, "w": self.w,
            "vnodes_per_node": self.ring.vnodes,
            "simulated_failure_seconds_left": max(0, round(self.failed_until - time.time(), 1)),
            "keys": self.store.count(),
            "hints_pending": hints,
            "ownership": self.ring.ownership(),
            "peers": {p: {"up": s["up"], "last_seen": s["last_seen"]} for p, s in self.health.items()},
            "stats": self.stats,
        }


class DynamoServicer(pb_grpc.DynamoNodeServicer):
    """gRPC endpoints other nodes call on this node."""

    def __init__(self, node: DynamoNode):
        self.node = node

    def _guard(self, context):
        if self.node.is_failed():
            context.abort(grpc.StatusCode.UNAVAILABLE, "node is (simulated) down")

    def Ping(self, request, context):
        self._guard(context)
        return pb.PingReply(node_id=self.node.node_id, keys=self.node.store.count(),
                            hints=sum(self.node.store.hint_counts().values()))

    def ReplicaPut(self, request, context):
        self._guard(context)
        ok = self.node.local_put(request.key, json.loads(request.versions_json), request.hint_for)
        return pb.PutReply(ok=ok, node_id=self.node.node_id)

    def ReplicaGet(self, request, context):
        self._guard(context)
        versions = self.node.local_get(request.key)
        return pb.GetReply(versions_json=json.dumps(versions or []), node_id=self.node.node_id,
                           found=versions is not None)

    def MerkleLeaves(self, request, context):
        self._guard(context)
        root, leaves = self.node.merkle(request.requester)
        return pb.MerkleReply(root=root, leaves=leaves)

    def SyncBucket(self, request, context):
        self._guard(context)
        incoming = json.loads(request.entries_json)
        for key, versions in incoming.items():
            if versions:
                self.node.store.merge_put(key, versions)
        mine = self.node.bucket_entries(request.requester, request.bucket)
        return pb.SyncReply(entries_json=json.dumps(mine))
