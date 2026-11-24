"""Failure-case walkthroughs required by the assignment.

    python -m smartdialer.demos            # all of them
    python -m smartdialer.demos crash      # one: crash | outage | agent_drop | duplicates | out_of_order

Each demo drives the real dialer code step by step with a scripted provider so
the output is deterministic, prints what happens, and returns facts that the
test-suite asserts on (tests/test_failures.py).
"""
from __future__ import annotations

import sys

from . import invariants
from .allocator import WorkerCrash
from .snapshot import take_snapshot
from .states import AgentState as A
from .testing import FixedPacer, World, seed_history


def _p(*a):
    print("   ", *a)


def _state(w, cid, agent=None):
    c = w.call(cid)
    s = f"call={c['state']}"
    if c["end_reason"]:
        s += f"({c['end_reason']})"
    a = agent or c["agent_id"]
    if a:
        s += f" agent {a}={w.agent(a)['state']}"
    b = w.q("SELECT state, attempts, system_failures FROM borrowers WHERE id=?", c["borrower_id"])[0]
    s += f" borrower={b[0]}"
    cmd = w.q("SELECT kind, status, owner, lease_until FROM commands WHERE call_id=? ORDER BY id", cid)
    s += " outbox=[" + ", ".join(f"{k}:{st}" + (f"@{o}" if st == "PENDING" and o else "") for k, st, o, _ in cmd) + "]"
    return s


# --------------------------------------------------------------------------- 1
def crash(verbose=True):
    out = print if verbose else (lambda *a: None)
    p = _p if verbose else (lambda *a: None)
    out("\n=== 1. Worker crash: agent reserved > borrower reserved > call initiated > worker dies ===")
    w = World(agents=1, workers=2)
    w0, w1 = w.workers

    def die(point, **kw):
        if point == "after_dial":
            raise WorkerCrash(point)
    w0.allocator.crash_hook = die
    try:
        w0.tick()
    except WorkerCrash:
        pass
    (c,) = w.calls()
    cid = c["id"]
    p(f"t+0.0  w0 reserved agent+borrower, provider ACCEPTED the dial, then w0 died before writing INITIATED")
    p(f"       {_state(w, cid)}")
    w.advance(0.5)
    w1.tick()
    p(f"t+0.5  w1 ticks: DIAL is still leased by dead w0, nothing to do      {_state(w, cid)}")
    w.advance(1.7)
    w1.agents.heartbeat("a0")
    w1.tick()
    p(f"t+2.2  w0's lease expired; w1 re-runs DIAL with the SAME idempotency key")
    p(f"       provider dial requests={len(w.provider.dial_requests)}, real calls at provider={len(w.provider.calls)}")
    p(f"       {_state(w, cid)}")
    w1.ingestor.ingest(w.provider.ev(cid, "ANSWERED", at=w.clock.now()))
    w1.allocator.dispatch()
    p(f"t+2.2  ANSWERED arrives (any worker can take webhooks)               {_state(w, cid)}")
    w1.ingestor.ingest(w.provider.ev(cid, "COMPLETED", answered=True))
    p(f"       COMPLETED                                                    {_state(w, cid)}")
    facts = dict(dial_requests=len(w.provider.dial_requests), provider_calls=len(w.provider.calls),
                 final=w.call(cid)["state"], agent=w.agent("a0")["state"],
                 invariants=invariants.check(w.store.read()))

    out("\n    -- variant: everyone is down for 10s (longer than the 5s permit TTL), webhooks lost --")
    w = World(agents=1, workers=2)
    w0, w1 = w.workers
    w0.allocator.crash_hook = die
    try:
        w0.tick()
    except WorkerCrash:
        pass
    (c,) = w.calls()
    cid = c["id"]
    w.advance(10)
    w1.agents.heartbeat("a0")
    w1.allocator.dispatch()
    p(f"t+10   approval is stale -> never re-dial; ask provider instead: it exists -> adopt   {_state(w, cid)}")
    facts["stale_adopted"] = w.call(cid)["state"]

    w = World(agents=1, workers=2)
    w0, w1 = w.workers

    def die_before(point, **kw):
        if point == "after_lease":
            raise WorkerCrash(point)
    w0.allocator.crash_hook = die_before
    try:
        w0.tick()
    except WorkerCrash:
        pass
    (c,) = w.calls()
    cid = c["id"]
    w.advance(10)
    w1.agents.heartbeat("a0")
    w1.tick()
    p(f"t+10   (w0 died BEFORE dialling) provider never heard of it -> cancel, release agent, no attempt charged")
    p(f"       {_state(w, cid, 'a0')}")
    facts["stale_unknown"] = w.call(cid)["state"]
    facts["stale_unknown_agent"] = w.agent("a0")["state"]
    return facts


# --------------------------------------------------------------------------- 2
def outage(verbose=True):
    out = print if verbose else (lambda *a: None)
    p = _p if verbose else (lambda *a: None)
    out("\n=== 2. Provider outage: provider starts timing out ===")
    w = World(agents=5, borrowers=200)
    w.w.tick()
    ids = [c["id"] for c in w.calls()]
    p(f"t=0    5 calls in flight, all INITIATED")
    w.provider.down = True
    p(f"t=0    provider goes dark: API times out, no webhooks")
    timeline = []
    for sec in range(1, 81):
        w.advance(1)
        for i in range(5):
            w.w.agents.heartbeat(f"a{i}")
        health_seen = w.w.router.health()          # what the Safety Controller saw for this decision
        d = w.w.tick()
        timeline.append((sec, health_seen, d.action, d.approved))
        if sec in (1, 5, 10, 21, 22, 30, 45):
            states = {}
            for c in w.calls():
                states[c["state"] + (f"/{c['end_reason']}" if c["end_reason"] else "")] = states.get(c["state"] + (f"/{c['end_reason']}" if c["end_reason"] else ""), 0) + 1
            p(f"t={sec:<3}  breaker={w.w.router.breakers['fake'].current():9} decision={d.action}({d.approved}/{d.requested})  calls={states}")
        if sec == 50:
            w.provider.down = False
            p(f"t=50   provider recovers")
        if sec in (55, 60, 75, 80):
            p(f"t={sec:<3}  breaker={w.w.router.breakers['fake'].current():9} decision={d.action}({d.approved}/{d.requested}) mode={d.mode} reasons={list(d.reasons)}")
    b = w.q("SELECT MAX(system_failures), MAX(attempts) FROM borrowers")[0]
    p(f"borrowers: max system_failures={b[0]} (retried with backoff), max real attempts charged={b[1]}")
    dead = w.q("SELECT COUNT(*) FROM commands WHERE status='DEAD'")[0][0]
    p(f"outbox: {dead} commands gave up after retries (HANGUP/DIAL during outage) and are visible for ops")
    return dict(timeline=timeline, existing=[w.call(i)["state"] for i in ids],
                invariants=invariants.check(w.store.read()), max_attempts=b[1])


# --------------------------------------------------------------------------- 3
def agent_drop(verbose=True):
    out = print if verbose else (lambda *a: None)
    p = _p if verbose else (lambda *a: None)
    out("\n=== 3. 100 agents available, 40 disappear within 3 seconds (predictive) ===")
    w = World(agents=100, borrowers=5000, pacer=FixedPacer(1000), workers=1)
    seed_history(w, 400, answer_rate=0.3)
    w.w.tick()
    snap = take_snapshot(w.store.read(), w.clock.now())
    p(f"t=0.0  steady state: available={snap.available}, unbound calls in flight={snap.unbound_inflight}")
    rows = []
    gone = 0
    for step in range(8):                          # 0.5s steps; worker ticks every 1s
        w.advance(0.5)
        if gone < 40:
            for i in range(gone, min(40, gone + 7)):
                w.w.agents.go_unavailable(f"a{i}")
            gone = min(40, gone + 7)
        for i in range(gone, 100):
            w.w.agents.heartbeat(f"a{i}")
        if step % 2 == 1:
            d = w.w.tick()
            snap = take_snapshot(w.store.read(), w.clock.now())
            shed = len([c for c in w.calls() if c["end_reason"] == "shed"])
            rows.append(dict(t=(step + 1) * 0.5, gone=gone, available=snap.available, inflight=snap.unbound_inflight,
                             shed_total=shed, approved=d.approved, action=d.action,
                             overflow_p=d.detail.get("overflow_p_after", d.detail.get("inflight_overflow_p"))))
            r = rows[-1]
            p(f"t={r['t']:<4} agents gone={r['gone']:<3} available={r['available']:<3} in-flight={r['inflight']:<4} "
              f"shed so far={r['shed_total']:<3} new approved={r['approved']:<3} ({r['action']})")
    p("Reaction = one worker tick (1s here). Every decision re-reads capacity inside its own transaction,")
    p("and the shed step cancels the newest unanswered calls when overflow risk exceeds the shed threshold.")
    return dict(rows=rows, invariants=invariants.check(w.store.read()))


# --------------------------------------------------------------------------- 4/5
def duplicates(verbose=True):
    out = print if verbose else (lambda *a: None)
    p = _p if verbose else (lambda *a: None)
    out("\n=== 4. Duplicate events: ANSWERED, ANSWERED, ANSWERED, COMPLETED ===")
    w = World(agents=1)
    w.w.tick()
    cid = w.calls()[0]["id"]
    effects = []
    for ev in [w.provider.ev(cid, "ANSWERED"), w.provider.ev(cid, "ANSWERED"),
               w.provider.ev(cid, "ANSWERED", event_id="semantic-dup"), w.provider.ev(cid, "COMPLETED", answered=True),
               w.provider.ev(cid, "COMPLETED", answered=True)]:
        e = w.ingest(ev)
        w.w.allocator.dispatch()
        effects.append(e)
        p(f"{ev.type:9} id={ev.event_id[-14:]:14} -> {e:24} {_state(w, cid)}")
    p(f"bridges sent to provider: {len(w.provider.bridges)}")
    return dict(effects=effects, bridges=len(w.provider.bridges), final=w.call(cid)["state"],
                invariants=invariants.check(w.store.read()))


def out_of_order(verbose=True):
    out = print if verbose else (lambda *a: None)
    p = _p if verbose else (lambda *a: None)
    out("\n=== 5. Out-of-order events: COMPLETED, ANSWERED, RINGING ===")
    w = World(agents=1)
    w.w.tick()
    cid = w.calls()[0]["id"]
    effects = []
    for ev in [w.provider.ev(cid, "COMPLETED", answered=True), w.provider.ev(cid, "ANSWERED"),
               w.provider.ev(cid, "RINGING")]:
        e = w.ingest(ev)
        w.w.allocator.dispatch()
        effects.append(e)
        p(f"{ev.type:9} -> {e:24} {_state(w, cid, 'a0')}")
    p("COMPLETED(answered) for a call we never bridged = the borrower heard silence: recorded as ABANDONED.")
    p("Later events cannot move a terminal call. The agent was released the moment the call ended.")
    out("\n    -- variant: RINGING after ANSWERED, ANSWERED before INITIATED was written --")
    w2 = World(agents=1)
    w2.w.allocator.allocate(w2.w.pacer.propose(take_snapshot(w2.store.read(), w2.clock.now())))
    cid2 = w2.calls()[0]["id"]
    for ev in [w2.provider.ev(cid2, "ANSWERED"), w2.provider.ev(cid2, "RINGING")]:
        e = w2.ingest(ev)
        p(f"{ev.type:9} -> {e:24} {_state(w2, cid2)}")
    w2.w.allocator.dispatch()
    p(f"the pending DIAL becomes a no-op, the BRIDGE runs   {_state(w2, cid2)}")
    return dict(effects=effects, final=w.call(cid)["state"], agent=w.agent("a0")["state"],
                variant=w2.call(cid2)["state"],
                invariants=invariants.check(w.store.read()) + invariants.check(w2.store.read()))


ALL = {"crash": crash, "outage": outage, "agent_drop": agent_drop, "duplicates": duplicates, "out_of_order": out_of_order}

if __name__ == "__main__":
    for name in (sys.argv[1:] or ALL):
        ALL[name]()
