"""In-process stand-in for `confluent_kafka`, used ONLY by the local end-to-end test.

Implements just the surface our services use (Producer, Consumer with consumer
groups and partition assignment, AdminClient, NewTopic) so the whole system can
be exercised in one Python process with a real Postgres but without a broker.
"""
import sys
import threading
import time
import types
import zlib
from concurrent.futures import Future

_lock = threading.RLock()
_topics: dict[str, list[list]] = {}          # topic -> partitions -> [Message]
_positions: dict[tuple, int] = {}            # (group, topic, partition) -> next offset
_members: dict[str, dict[int, "Consumer"]] = {}  # group -> {id: consumer}
FAIL_PRODUCE = False                          # tests flip this to simulate a broker outage


def messages(topic: str) -> list[dict]:
    """All JSON messages written to `topic` so far (for assertions)."""
    import json
    with _lock:
        out = []
        for part in _topics.get(topic, []):
            for m in part:
                try:
                    out.append(json.loads(m.value()))
                except Exception:
                    pass
        return out


def partition_of(topic: str, key: str) -> list[dict]:
    """Messages with `key` in log order (Kafka guarantees order per partition)."""
    import json
    with _lock:
        return [json.loads(m.value()) for part in _topics.get(topic, []) for m in part
                if m.key() == key.encode()]


class KafkaException(Exception):
    pass


class KafkaError:
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class Message:
    def __init__(self, topic, partition, offset, key, value, ts):
        self._t, self._p, self._o, self._k, self._v, self._ts = topic, partition, offset, key, value, ts

    def value(self): return self._v
    def key(self): return self._k
    def error(self): return None
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def timestamp(self): return (1, self._ts)


def create_topic(name, partitions=3):
    with _lock:
        _topics.setdefault(name, [[] for _ in range(partitions)])


class _Meta:
    def __init__(self, topics):
        self.topics = {t: types.SimpleNamespace(partitions={i: None for i in range(len(p))}) for t, p in topics.items()}
        self.brokers = {1: None, 2: None}


class Producer:
    def __init__(self, conf):
        self.conf = conf

    def produce(self, topic, value=None, key=None, on_delivery=None, timestamp=None, partition=None):
        if FAIL_PRODUCE:
            if on_delivery:
                on_delivery(KafkaError("simulated broker outage"), None)
            return
        with _lock:
            parts = _topics.setdefault(topic, [[] for _ in range(6)])
            if partition is None:
                kb = key.encode() if isinstance(key, str) else (key or b"")
                partition = (zlib.crc32(kb) % len(parts)) if kb else int(time.time() * 1000) % len(parts)
            v = value.encode() if isinstance(value, str) else value
            k = key.encode() if isinstance(key, str) else key
            msg = Message(topic, partition, len(parts[partition]), k, v, timestamp or int(time.time() * 1000))
            parts[partition].append(msg)
        if on_delivery:
            on_delivery(None, msg)

    def poll(self, timeout=0): return 0
    def flush(self, timeout=None): return 0
    def list_topics(self, topic=None, timeout=None):
        with _lock:
            return _Meta(_topics)


class Consumer:
    _ids = 0

    def __init__(self, conf):
        self.group = conf["group.id"]
        self.reset = conf.get("auto.offset.reset", "earliest")
        Consumer._ids += 1
        self.id = Consumer._ids
        self.topics = []
        self.rr = 0

    def subscribe(self, topics):
        self.topics = topics
        with _lock:
            _members.setdefault(self.group, {})[self.id] = self
            for t in topics:
                create_topic(t, 6)

    def _assigned(self):
        members = sorted(_members.get(self.group, {}))
        idx = members.index(self.id)
        out = []
        for t in self.topics:
            for p in range(len(_topics[t])):
                if p % len(members) == idx:
                    out.append((t, p))
        return out

    def poll(self, timeout=1.0):
        deadline = time.time() + timeout
        while True:
            with _lock:
                assigned = self._assigned()
                for i in range(len(assigned)):
                    t, p = assigned[(self.rr + i) % len(assigned)]
                    key = (self.group, t, p)
                    if key not in _positions:
                        _positions[key] = 0 if self.reset == "earliest" else len(_topics[t][p])
                    pos = _positions[key]
                    if pos < len(_topics[t][p]):
                        _positions[key] = pos + 1
                        self.rr += 1
                        return _topics[t][p][pos]
            if time.time() >= deadline:
                return None
            time.sleep(0.01)

    def store_offsets(self, msg): pass
    def commit(self, *a, **k): pass

    def close(self):
        with _lock:
            _members.get(self.group, {}).pop(self.id, None)


class NewTopic:
    def __init__(self, topic, num_partitions=1, replication_factor=1):
        self.topic, self.num_partitions = topic, num_partitions


class AdminClient:
    def __init__(self, conf): pass

    def list_topics(self, timeout=None):
        with _lock:
            return _Meta(_topics)

    def create_topics(self, topics):
        out = {}
        for nt in topics:
            create_topic(nt.topic, nt.num_partitions)
            f = Future()
            f.set_result(None)
            out[nt.topic] = f
        return out


def install():
    mod = types.ModuleType("confluent_kafka")
    for name in ("Producer", "Consumer", "KafkaException", "KafkaError", "Message"):
        setattr(mod, name, globals()[name])
    admin = types.ModuleType("confluent_kafka.admin")
    admin.AdminClient, admin.NewTopic = AdminClient, NewTopic
    mod.admin = admin
    sys.modules["confluent_kafka"] = mod
    sys.modules["confluent_kafka.admin"] = admin
