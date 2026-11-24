"""The five failure cases from the assignment, asserted (see smartdialer/demos.py for the narrated version)."""
from smartdialer import demos


def test_worker_crash_recovery():
    f = demos.crash(verbose=False)
    assert f["dial_requests"] == 2 and f["provider_calls"] == 1     # re-dial was idempotent
    assert f["final"] == "COMPLETED" and f["agent"] == "WRAP_UP"
    assert f["stale_adopted"] == "INITIATED"                        # stale approval: adopted, not re-dialled
    assert f["stale_unknown"] == "CANCELLED" and f["stale_unknown_agent"] == "AVAILABLE"
    assert f["invariants"] == []


def test_provider_outage():
    f = demos.outage(verbose=False)
    health = {t: h for t, h, _, _ in f["timeline"]}
    assert "DOWN" in health.values()
    assert all(s == "FAILED" for s in f["existing"])               # in-flight calls failed closed
    for t, h, action, approved in f["timeline"]:
        if h == "DOWN":
            assert approved == 0                                  # no new calls while down
        if h == "DEGRADED":
            assert approved <= 2                                  # probe trickle only
    assert f["timeline"][-1][1] == "HEALTHY"                      # recovers by itself
    assert f["max_attempts"] == 0                                 # outage not charged to borrowers
    assert f["invariants"] == []


def test_agent_drop_reaction_within_one_tick():
    f = demos.agent_drop(verbose=False)
    after = [r for r in f["rows"] if r["gone"] >= 28]
    assert after[0]["shed_total"] > 0 and after[0]["approved"] == 0
    assert f["rows"][-1]["inflight"] < f["rows"][0]["inflight"]
    assert f["invariants"] == []


def test_duplicate_events_single_transition():
    f = demos.duplicates(verbose=False)
    assert f["effects"].count("connected_bound_agent") == 1
    assert f["effects"].count("completed") == 1
    assert f["bridges"] == 1 and f["final"] == "COMPLETED" and f["invariants"] == []


def test_out_of_order_events_consistent():
    f = demos.out_of_order(verbose=False)
    assert f["final"] == "ABANDONED" and f["agent"] == "AVAILABLE"
    assert f["variant"] == "CONNECTED"
    assert f["invariants"] == []
