"""Duplicate, out-of-order and crash-interrupted provider events."""
import pytest

from smartdialer.allocator import WorkerCrash
from smartdialer.testing import World
from smartdialer import invariants


def _live_call(w):
    w.w.tick()
    (c,) = w.calls()
    return c["id"]


def test_answered_x3_then_completed():
    w = World(agents=1)
    cid = _live_call(w)
    effects = [w.ingest(w.provider.ev(cid, "ANSWERED")) for _ in range(3)]
    assert effects == ["connected_bound_agent", "duplicate_delivery", "duplicate_delivery"]
    # semantic duplicate: same event, different provider id
    assert w.ingest(w.provider.ev(cid, "ANSWERED", event_id="other-id")) == "ignored_stale"
    w.w.allocator.dispatch()
    assert len(w.provider.bridges) == 1
    assert w.ingest(w.provider.ev(cid, "COMPLETED", answered=True)) == "completed"
    assert w.call(cid)["state"] == "COMPLETED" and w.agent("a0")["state"] == "WRAP_UP"
    assert invariants.check(w.store.read()) == []


def test_completed_answered_ringing_out_of_order():
    w = World(agents=1)
    cid = _live_call(w)
    # COMPLETED (borrower picked up and hung up) before we ever saw ANSWERED:
    # nobody was bridged, so this was an abandon, and the agent goes back to work.
    assert w.ingest(w.provider.ev(cid, "COMPLETED", answered=True)) == "abandoned"
    assert w.ingest(w.provider.ev(cid, "ANSWERED")) == "late_answer_on_terminal"
    assert w.ingest(w.provider.ev(cid, "RINGING")) == "ignored_stale"
    assert w.call(cid)["state"] == "ABANDONED"
    assert w.agent("a0")["state"] == "AVAILABLE"
    assert invariants.check(w.store.read()) == []


def test_completed_without_answer_before_ringing():
    w = World(agents=1)
    cid = _live_call(w)
    assert w.ingest(w.provider.ev(cid, "COMPLETED", answered=False, reason="no_answer")) == "failed"
    assert w.ingest(w.provider.ev(cid, "RINGING")) == "ignored_stale"
    assert w.call(cid)["state"] == "FAILED" and w.agent("a0")["state"] == "AVAILABLE"


def test_ringing_while_still_reserved_adopts_call():
    """Worker died between provider.dial() and writing INITIATED; webhook arrives."""
    w = World(agents=1)
    w.w.allocator.allocate(w.w.pacer.propose(__import__("smartdialer.snapshot", fromlist=["x"]).take_snapshot(w.store.read(), w.clock.now())))
    (c,) = w.calls()
    assert c["state"] == "RESERVED"
    assert w.ingest(w.provider.ev(c["id"], "RINGING")) == "applied"
    assert w.call(c["id"])["state"] == "RINGING" and w.agent("a0")["state"] == "DIALING"


def test_failed_after_answer_is_ignored():
    w = World(agents=1)
    cid = _live_call(w)
    w.ingest(w.provider.ev(cid, "ANSWERED"))
    assert w.ingest(w.provider.ev(cid, "FAILED", reason="provider_error")) == "ignored_contradictory"
    assert w.call(cid)["state"] == "CONNECTED"


def test_event_for_unknown_call_is_recorded_not_applied():
    w = World(agents=1)
    assert w.ingest(w.provider.ev("nope", "ANSWERED")) == "unknown_call"


def test_crash_before_commit_rolls_back_and_redelivery_applies_once():
    w = World(agents=1)
    cid = _live_call(w)

    def boom(point, **kw):
        if point == "before_commit":
            raise WorkerCrash(point)
    w.w.ingestor.crash_hook = boom
    with pytest.raises(WorkerCrash):
        w.ingest(w.provider.ev(cid, "ANSWERED"))
    assert w.call(cid)["state"] == "INITIATED"            # nothing happened
    assert w.q("SELECT COUNT(*) FROM provider_events")[0][0] == 0
    w.w.ingestor.crash_hook = lambda *a, **k: None
    assert w.ingest(w.provider.ev(cid, "ANSWERED")) == "connected_bound_agent"


def test_crash_after_answered_commit_then_completed():
    """'The provider sends ANSWERED, your worker crashes, then COMPLETED arrives.'"""
    w = World(agents=1, workers=2)
    cid = _live_call(w)
    w0, w1 = w.workers

    def boom(point, **kw):
        if point == "after_commit":
            raise WorkerCrash(point)
    w0.ingestor.crash_hook = boom
    with pytest.raises(WorkerCrash):
        w0.ingestor.ingest(w.provider.ev(cid, "ANSWERED"))
    # committed: call+agent CONNECTED, BRIDGE in the outbox; w0 is gone.
    assert w.call(cid)["state"] == "CONNECTED"
    assert w1.ingestor.ingest(w.provider.ev(cid, "ANSWERED")) == "duplicate_delivery"   # provider redelivers
    w1.allocator.dispatch()                                  # survivor executes the BRIDGE
    assert w.provider.bridges == [(cid, "a0")]
    assert w1.ingestor.ingest(w.provider.ev(cid, "COMPLETED", answered=True)) == "completed"
    assert w.agent("a0")["state"] == "WRAP_UP"
    assert invariants.check(w.store.read()) == []
