from smartdialer.providers.base import ProviderError, ProviderTimeout
from smartdialer.states import AgentState as A
from smartdialer.testing import FixedPacer, World
from smartdialer import invariants


def test_never_more_agent_bound_calls_than_available_agents():
    w = World(agents=50, borrowers=2000, pacer=FixedPacer(500, "progressive"))
    for _ in range(5):
        w.w.tick()
    assert len(w.calls()) == 50
    assert len({c["agent_id"] for c in w.calls()}) == 50
    assert all(w.agent(f"a{i}")["state"] == "DIALING" for i in range(50))
    assert len(w.provider.dial_requests) == 50
    assert invariants.check(w.store.read()) == []


def test_happy_path_agent_and_call_lifecycle():
    w = World(agents=1)
    w.w.tick()
    (c,) = w.calls()
    assert w.agent("a0")["state"] == "DIALING" and c["state"] == "INITIATED"
    w.ingest(w.provider.ev(c["id"], "RINGING"))
    w.ingest(w.provider.ev(c["id"], "ANSWERED", at=w.clock.now()))
    assert w.call(c["id"])["state"] == "CONNECTED" and w.agent("a0")["state"] == "CONNECTED"
    w.w.allocator.dispatch()
    assert w.provider.bridges == [(c["id"], "a0")]
    w.advance(60)
    w.ingest(w.provider.ev(c["id"], "COMPLETED", answered=True))
    assert w.call(c["id"])["state"] == "COMPLETED" and w.agent("a0")["state"] == "WRAP_UP"
    assert w.q("SELECT state FROM borrowers WHERE id=?", c["borrower_id"])[0][0] == "DONE"
    w.advance(11)
    w.w.agents.heartbeat("a0")
    w.w.reconciler.run()                      # system-timed wrap-up ends
    assert w.agent("a0")["state"] == "AVAILABLE"


def test_failed_call_releases_agent_and_schedules_retry():
    w = World(agents=1)
    w.w.tick()
    (c,) = w.calls()
    w.ingest(w.provider.ev(c["id"], "FAILED", reason="busy"))
    assert w.agent("a0")["state"] == "AVAILABLE"
    b = w.q("SELECT * FROM borrowers WHERE id=?", c["borrower_id"])[0]
    assert b["state"] == "READY" and b["attempts"] == 1 and b["next_attempt_at"] > w.clock.now()


def test_provider_error_is_not_charged_to_the_borrower():
    w = World(agents=1)
    w.provider.fail_next = [ProviderError("rejected")]
    w.w.tick()
    (c,) = w.calls()
    assert c["state"] == "FAILED" and c["end_reason"] == "provider_error"
    b = w.q("SELECT * FROM borrowers WHERE id=?", c["borrower_id"])[0]
    assert b["attempts"] == 0 and b["system_failures"] == 1
    assert w.agent("a0")["state"] == "AVAILABLE"


def test_dial_timeout_retried_with_same_idempotency_key_rings_once():
    w = World(agents=1)
    w.provider.create_on_timeout = True              # the timed-out request actually created the call
    w.provider.fail_next = [ProviderTimeout("slow")]
    w.w.tick()
    (c,) = w.calls()
    assert c["state"] == "RESERVED"
    w.advance(1.1)                                   # retry backoff
    w.w.allocator.dispatch()
    assert w.call(c["id"])["state"] == "INITIATED"
    assert w.provider.dial_requests == [c["id"], c["id"]]   # same key twice...
    assert len(w.provider.calls) == 1                       # ...one real call


def test_agent_disappears_during_setup_call_rebound_to_another_agent():
    w = World(agents=2, pacer=FixedPacer(1, "progressive"))
    w.w.tick()
    (c,) = w.calls()
    owner = c["agent_id"]
    other = "a1" if owner == "a0" else "a0"
    r = w.w.agents.go_unavailable(owner)
    assert "rebound" in r
    assert w.call(c["id"])["agent_id"] == other
    assert w.agent(other)["state"] == "DIALING" and w.agent(owner)["state"] == "OFFLINE"
    assert invariants.check(w.store.read()) == []


def test_agent_disappears_during_setup_no_spare_agent_call_cancelled():
    w = World(agents=1)
    w.w.tick()
    (c,) = w.calls()
    r = w.w.agents.go_unavailable("a0", A.PAUSED)
    assert r.endswith("call cancelled")
    assert w.call(c["id"])["state"] == "CANCELLED"
    w.w.allocator.dispatch()
    assert w.provider.hangups == [c["id"]]
    b = w.q("SELECT * FROM borrowers WHERE id=?", c["borrower_id"])[0]
    assert b["state"] == "READY" and b["attempts"] == 0     # our problem, not theirs
    # the borrower answers a moment later anyway: counted honestly, line torn down
    assert w.ingest(w.provider.ev(c["id"], "ANSWERED")) == "late_answer_on_terminal"


def test_logout_while_connected_is_deferred_until_wrap_up():
    w = World(agents=1)
    w.w.tick()
    (c,) = w.calls()
    w.ingest(w.provider.ev(c["id"], "ANSWERED"))
    assert w.w.agents.go_unavailable("a0") == "deferred_until_call_ends"
    assert w.agent("a0")["state"] == "CONNECTED"
    w.ingest(w.provider.ev(c["id"], "COMPLETED", answered=True))
    w.advance(11)
    w.w.reconciler.run()
    assert w.agent("a0")["state"] == "OFFLINE"


def test_heartbeat_loss_treated_as_disappearance():
    w = World(agents=1)
    w.w.tick()
    w.advance(16)
    w.w.reconciler.run()
    assert w.agent("a0")["state"] == "OFFLINE"
    assert w.calls()[0]["state"] == "CANCELLED"
