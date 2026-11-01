"""Consistent-hash ring with virtual nodes (Dynamo §4.2).

Each physical node owns `vnodes` tokens on a 128-bit MD5 ring. A key's
*preference list* is the first N distinct physical nodes met walking clockwise
from hash(key). Virtual nodes spread load evenly and make membership changes
move only ~1/|nodes| of the keys.
"""
import bisect
import hashlib


def h(value: str) -> int:
    return int.from_bytes(hashlib.md5(value.encode()).digest(), "big")


class Ring:
    def __init__(self, nodes: list[str], vnodes: int = 64):
        self.nodes = sorted(nodes)
        self.vnodes = vnodes
        self._tokens = sorted((h(f"{node}#vn{i}"), node) for node in self.nodes for i in range(vnodes))
        self._hashes = [t for t, _ in self._tokens]

    def walk(self, key: str):
        """Yield distinct physical nodes clockwise from the key's position."""
        if not self._tokens:
            return
        start = bisect.bisect_right(self._hashes, h(key)) % len(self._tokens)
        seen = set()
        for i in range(len(self._tokens)):
            node = self._tokens[(start + i) % len(self._tokens)][1]
            if node not in seen:
                seen.add(node)
                yield node
                if len(seen) == len(self.nodes):
                    return

    def preference_list(self, key: str, n: int) -> list[str]:
        out = []
        for node in self.walk(key):
            out.append(node)
            if len(out) == n:
                break
        return out

    def ownership(self) -> dict[str, float]:
        """Fraction of the hash space owned (as primary) by each node."""
        space = 2 ** 128
        own = {n: 0 for n in self.nodes}
        for i, (token, node) in enumerate(self._tokens):
            prev = self._tokens[i - 1][0] if i else self._tokens[-1][0] - space
            own[node] += token - prev
        return {n: round(v / space, 4) for n, v in own.items()}
