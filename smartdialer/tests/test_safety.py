import math

import pytest

from smartdialer.safety import (OverflowRisk, SafetyConfig, SafetyController, binom_tail_gt,
                                max_calls_within_risk, wilson_upper)
from smartdialer.snapshot import take_snapshot
from smartdialer.testing import FixedPacer, World, seed_history


def test_overflow_risk_matches_binomial():
    for cap, p, n in [(5, 0.3, 12), (10, 0.2, 40), (3, 0.7, 6)]:
        r = OverflowRisk(cap)
        for _ in range(n):
            r.add(p)
        assert math.isclose(r.over, binom_tail_gt(n, p, cap), rel_tol=1e-9, abs_tol=1e-12)


def test_max_calls_bound_is_tight():
    n = max_calls_within_risk(10, 0.3, 0.02, 1000)
    assert binom_tail_gt(n, 0.3, 10) <= 0.02 < binom_tail_gt(n + 1, 0.3, 10)


def test_wilson_upper_is_above_point_estimate():
    assert wilson_upper(30, 100, 1.645) > 0.30
    assert wilson_upper(0, 0, 1.645) == 1.0


def test_predictive_without_history_falls_back_to_progressive():
    w = World(agents=5, pacer=FixedPacer(100))
    d = w.w.tick()
    assert d.action == "FALLBACK_PROGRESSIVE" and d.mode == "progressive" and d.approved == 5
    assert all(c["agent_id"] for c in w.calls())        # agent-bound, like progressive


def test_safety_caps_an_absurd_request_with_a_reason():
    w = World(agents=10, borrowers=3000, pacer=FixedPacer(1000))
    seed_history(w, 300, answer_rate=0.3)
    d = w.w.tick()
    assert d.mode == "predictive" and d.action == "REDUCE"
    assert 10 < d.approved < 60
    assert d.detail["overflow_p_after"] <= SafetyConfig().risk_epsilon
    assert any("risk_bound" in r or "hard_cap" in r for r in d.reasons)
    assert all(c["agent_id"] is None for c in w.calls())  # predictive: bound on answer


def test_answer_rate_spike_forces_fallback():
    w = World(agents=10, borrowers=3000, pacer=FixedPacer(50))
    seed_history(w, 400, answer_rate=0.2)
    # the last two minutes: everyone answers
    now = w.clock.now()
    with w.store.tx() as c:
        for i in range(120):
            c.execute("INSERT INTO borrowers(id, phone, state) VALUES (?, '+0', 'DONE')", (f"s{i}",))
            c.execute("""INSERT INTO calls(id, mode, borrower_id, state, created_at, updated_at, answered_at,
                         connected_at, ended_at, end_reason, was_answered)
                         VALUES (?, 'predictive', ?, 'COMPLETED', ?, ?, ?, ?, ?, 'normal', 1)""",
                      (f"s{i}", f"s{i}", now - 60, now - 50, now - 50, now - 50, now - 10))
    d = w.w.tick()
    assert d.action == "FALLBACK_PROGRESSIVE"
    assert any("answer_rate_rising" in r for r in d.reasons)


def test_high_abandon_rate_forces_fallback():
    w = World(agents=10, borrowers=3000, pacer=FixedPacer(50))
    seed_history(w, 300, answer_rate=0.3)
    with w.store.tx() as c:
        c.execute("UPDATE calls SET state='ABANDONED' WHERE id IN (SELECT id FROM calls WHERE was_answered=1 LIMIT 5)")
    d = w.w.tick()
    assert d.action == "FALLBACK_PROGRESSIVE" and any("abandon_rate_high" in r for r in d.reasons)


def test_all_providers_down_rejects_everything():
    w = World(agents=5, pacer=FixedPacer(5, "progressive"))
    w.provider.down = True
    for _ in range(10):
        w.w.tick()
        w.advance(1.1)
    assert w.w.router.health() == "DOWN"
    d = w.w.tick()
    assert d.action == "REJECT" and d.approved == 0


def test_permit_expires_if_not_dialled_in_time():
    w = World(agents=1)
    d = w.w.allocator.allocate(w.w.pacer.propose(take_snapshot(w.store.read(), w.clock.now())))
    assert d.approved == 1
    w.advance(6)                                   # nobody dispatched it within permit_ttl_s
    w.w.agents.heartbeat("a0")
    w.w.allocator.dispatch()
    (c,) = w.calls()
    assert c["state"] == "CANCELLED" and c["end_reason"] == "permit_expired"
    assert w.provider.dial_requests == []          # never rang anyone on a stale approval
    assert w.agent("a0")["state"] == "AVAILABLE"


def test_safety_cannot_be_switched_off_from_outside():
    sc = SafetyController(SafetyConfig(), lambda: "HEALTHY")
    with pytest.raises(AttributeError):
        sc.enabled = False                         # __slots__: no new attributes
    with pytest.raises(Exception):
        sc.cfg.risk_epsilon = 1.0                  # frozen config
    with pytest.raises(AttributeError):
        sc.cfg = SafetyConfig(risk_epsilon=1.0)    # read-only property


def test_pacer_has_no_path_to_the_provider():
    """Structural check: the pacing module imports nothing that can write or dial."""
    import ast, pathlib
    import smartdialer.pacing as pacing
    tree = ast.parse(pathlib.Path(pacing.__file__).read_text())
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not any(m and any(x in m for x in ("allocator", "provider", "safety", "store", "calls", "sqlite"))
                   for m in imported), imported


def test_capacity_collapse_sheds_unanswered_predictive_calls():
    w = World(agents=20, borrowers=3000, pacer=FixedPacer(1000))
    seed_history(w, 300, answer_rate=0.3)
    w.w.tick()
    inflight = len(w.calls())
    assert inflight > 20
    for i in range(12):                           # 12 of 20 agents vanish
        w.w.agents.go_unavailable(f"a{i}")
    w.w.pacer.n = 0
    w.w.tick()
    left = len([c for c in w.calls() if c["state"] != "CANCELLED"])
    shed = len([c for c in w.calls() if c["end_reason"] == "shed"])
    assert shed > 0 and left < inflight
    w.w.allocator.dispatch()
    assert len(w.provider.hangups) == shed


def test_answered_with_no_agent_is_abandoned_after_grace_and_hung_up():
    w = World(agents=4, borrowers=3000, pacer=FixedPacer(1000))
    seed_history(w, 300, answer_rate=0.3)
    w.w.tick()
    w.w.agents.go_unavailable("a2", target=__import__("smartdialer.states", fromlist=["x"]).AgentState.PAUSED)
    w.w.agents.go_unavailable("a3")
    ids = [c["id"] for c in w.calls()]
    for cid in ids[:3]:                           # 3 answers, 2 agents
        w.ingest(w.provider.ev(cid, "ANSWERED", at=w.clock.now()))
    assert w.q("SELECT COUNT(*) FROM calls WHERE state='CONNECTED'")[0][0] == 2
    waiting = w.q("SELECT id FROM calls WHERE state='ANSWERED'")[0][0]
    w.advance(2.5)
    for a in ("a0", "a1", "a2", "a3"):
        w.w.agents.heartbeat(a)
    w.w.reconciler.run()
    assert w.call(waiting)["state"] == "ABANDONED"
    w.w.allocator.dispatch()
    assert waiting in w.provider.hangups


def test_waiting_answered_call_gets_the_next_free_agent_first():
    w = World(agents=3, borrowers=3000, pacer=FixedPacer(1000))
    seed_history(w, 300, answer_rate=0.3)
    w.w.tick()
    w.w.agents.go_unavailable("a1")
    w.w.agents.go_unavailable("a2")
    a, b = [c["id"] for c in w.calls()][:2]
    w.ingest(w.provider.ev(a, "ANSWERED", at=w.clock.now()))
    w.ingest(w.provider.ev(b, "ANSWERED", at=w.clock.now()))
    assert w.call(b)["state"] == "ANSWERED"
    w.ingest(w.provider.ev(a, "COMPLETED", answered=True))
    w.w.agents.go_available("a0")                 # agent skips wrap-up
    assert w.call(b)["state"] == "CONNECTED"
