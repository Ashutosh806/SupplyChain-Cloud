"""Fast unit tests for the Dynamo-style store building blocks and the shipment state CRDT.

    pytest tests/test_dynamo_unit.py
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "delivery-status"))

from app.dynamo import vclock  # noqa: E402
from app.dynamo.ring import Ring  # noqa: E402
from app import shipment_state as ss  # noqa: E402


def test_vector_clock_ordering():
    a = {"n1": 1}
    b = vclock.increment(a, "n1")
    c = vclock.increment(a, "n2")
    assert vclock.compare(a, b) == vclock.BEFORE
    assert vclock.compare(b, a) == vclock.AFTER
    assert vclock.compare(b, c) == vclock.CONCURRENT
    assert vclock.compare(b, dict(b)) == vclock.EQUAL
    assert vclock.merge(b, c) == {"n1": 2, "n2": 1}


def test_reconcile_keeps_only_concurrent_siblings():
    v0 = {"clock": {"n1": 1}, "value": 0}
    v1 = {"clock": {"n1": 2}, "value": 1}                 # descends from v0
    v2 = {"clock": {"n1": 1, "n2": 1}, "value": 2}        # concurrent with v1
    out = vclock.reconcile([v0, v1, v2, dict(v1)])
    assert sorted(v["value"] for v in out) == [1, 2]
    merged = {"clock": vclock.increment(vclock.merge(v1["clock"], v2["clock"]), "n3"), "value": 3}
    assert vclock.reconcile(out + [merged]) == [merged]


def test_ring_preference_lists_are_distinct_and_stable():
    ring = Ring([f"n{i}" for i in range(5)], vnodes=128)
    for k in range(200):
        pref = ring.preference_list(f"shipment:SCT{k}", 3)
        assert len(set(pref)) == 3
        assert pref == Ring([f"n{i}" for i in reversed(range(5))], vnodes=128).preference_list(f"shipment:SCT{k}", 3)
    shares = ring.ownership()
    assert abs(sum(shares.values()) - 1) < 1e-3 and min(shares.values()) > 0.1


def test_adding_a_node_moves_only_a_fraction_of_keys():
    keys = [f"shipment:{i}" for i in range(2000)]
    before = Ring([f"n{i}" for i in range(5)])
    after = Ring([f"n{i}" for i in range(6)])
    moved = sum(before.preference_list(k, 1) != after.preference_list(k, 1) for k in keys)
    assert moved / len(keys) < 0.3   # ~1/6 expected with consistent hashing (vs ~5/6 with mod-N)


def _ev(i, status, t):
    return {"event_id": f"e{i}", "event_type": "CarrierStatusReported", "occurred_at": t,
            "payload": {"status": status, "event_time": t, "carrier": "DHL", "location": f"L{i}"}}


def test_state_merge_is_a_crdt_and_order_independent():
    random.seed(7)
    statuses = ["PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"]
    events = [_ev(i, random.choice(statuses), random.randint(0, 10**6)) for i in range(300)]
    for _ in range(20):
        random.shuffle(events)
        parts = [events[:100], events[100:200], events[200:]]
        vals = []
        for part in parts:
            v = ss.empty("T")
            for e in part:
                v = ss.apply_event(v, e)
            vals.append(v)
        m1 = ss.merge_values([ss.merge_values(vals[:2], "T"), vals[2]], "T")
        m2 = ss.merge_values([vals[2], ss.merge_values([vals[1], vals[0]], "T")], "T")
        assert m1 == m2 == ss.merge_values([m1, m1], "T")
        assert len(m1["events"]) <= ss.MAX_EVENTS
    d = ss.derive(m1)
    assert d["status"] == "DELIVERED"
    assert d["picked_up_at"] == min(e["occurred_at"] for e in events if e["payload"]["status"] == "PICKED_UP")


def test_out_of_order_scans_do_not_regress_status():
    v = ss.empty("T")
    v = ss.apply_event(v, _ev(1, "OUT_FOR_DELIVERY", 300))
    v = ss.apply_event(v, _ev(2, "PICKED_UP", 100))          # late, older scan
    d = ss.derive(v)
    assert d["status"] == "OUT_FOR_DELIVERY" and d["last_location"] == "L1"
