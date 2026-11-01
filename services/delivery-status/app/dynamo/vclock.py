"""Vector clocks — causality tracking for versions written by different coordinators.

A clock is a dict {node_id: counter}. Two versions are either causally ordered
(one *descends* from the other and supersedes it) or *concurrent*, in which case
both are kept as siblings until they are reconciled (Dynamo §4.4).
"""
import hashlib
import json

BEFORE, AFTER, EQUAL, CONCURRENT = "before", "after", "equal", "concurrent"


def compare(a: dict, b: dict) -> str:
    keys = set(a) | set(b)
    a_le_b = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    b_le_a = all(b.get(k, 0) <= a.get(k, 0) for k in keys)
    if a_le_b and b_le_a:
        return EQUAL
    if a_le_b:
        return BEFORE
    if b_le_a:
        return AFTER
    return CONCURRENT


def merge(*clocks: dict) -> dict:
    out: dict = {}
    for c in clocks:
        for k, v in c.items():
            out[k] = max(out.get(k, 0), v)
    return out


def increment(clock: dict, node_id: str) -> dict:
    out = dict(clock)
    out[node_id] = out.get(node_id, 0) + 1
    return out


def reconcile(versions: list[dict]) -> list[dict]:
    """Syntactic reconciliation: drop versions dominated by (or equal to) another.

    What is left is the set of mutually concurrent siblings (usually exactly one).
    """
    kept: list[dict] = []
    for v in versions:
        skip = False
        for k in kept:
            rel = compare(v["clock"], k["clock"])
            if rel in (BEFORE, EQUAL):
                skip = True
                break
        if skip:
            continue
        kept = [k for k in kept if compare(k["clock"], v["clock"]) != BEFORE]
        kept.append(v)
    return sorted(kept, key=lambda v: json.dumps(v["clock"], sort_keys=True))


def digest(versions: list[dict]) -> str:
    """Stable fingerprint of a version set (used for Merkle leaves and read repair)."""
    clocks = sorted(json.dumps(v["clock"], sort_keys=True) for v in versions)
    return hashlib.sha256("|".join(clocks).encode()).hexdigest()[:32]
