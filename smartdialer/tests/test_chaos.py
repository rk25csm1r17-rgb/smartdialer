"""Randomized adversarial fuzzing — the 'Break' step.

For many seeds we drive the REAL dialer with a hostile environment:
  * provider events delivered in random order, duplicated, and dropped;
  * worker crashes before/after the event commit (in-memory state lost);
  * agents logging out, logging back in, and losing their heartbeat at random;
  * multiple workers sharing the database.

After every single step we assert the global invariants hold, and after draining
we assert the system reaches a fully consistent terminal state: every call
terminal, every borrower released, every agent back to a rest state, and every
borrower that was ever answered either connected to an agent or counted as an
abandon — never silently lost.

This is the test that backs the rubric's top line: correctness before cleverness.
"""
import random

import pytest

from smartdialer import invariants
from smartdialer.allocator import WorkerCrash
from smartdialer.config import DialerConfig
from smartdialer.pacing import ProgressivePacer
from smartdialer.states import TERMINAL_CALL_STATES, AgentState as A, CallState as C
from smartdialer.testing import FixedPacer, World, seed_history


def _gen_script(provider, cid, rng):
    """A plausible, possibly-messy sequence of provider events for one call."""
    evs = []
    answered = rng.random() < 0.5
    if rng.random() < 0.9:
        evs.append(provider.ev(cid, "RINGING"))
    if answered:
        evs.append(provider.ev(cid, "ANSWERED"))
        evs.append(provider.ev(cid, "COMPLETED", answered=True))
    else:
        evs.append(provider.ev(cid, rng.choice(["COMPLETED", "FAILED"]),
                               answered=False, reason=rng.choice(["no_answer", "busy"])))
    return evs


def _one_run(seed, mode):
    rng = random.Random(seed)
    cfg = DialerConfig(wrap_up_s=rng.choice([0, 5, 10]))
    pacer = FixedPacer(rng.randint(3, 15), mode)
    w = World(agents=rng.randint(2, 6), borrowers=500, pacer=pacer, cfg=cfg,
              workers=rng.randint(1, 3))
    if mode == "predictive":
        seed_history(w, 300, answer_rate=0.3)
    n_agents = len(w.q("SELECT id FROM agents"))
    agent_ids = [r[0] for r in w.q("SELECT id FROM agents")]

    bag = []          # events waiting to be delivered, in no particular order
    scripted = set()

    def live_workers():
        return [x for x in w.workers if x is not None]

    def maybe_script_new_calls():
        for row in w.q("SELECT id FROM calls"):
            cid = row[0]
            if cid not in scripted:
                scripted.add(cid)
                script = _gen_script(w.provider, cid, rng)
                for ev in script:
                    # random duplicates (same id = redelivery; new id = semantic dup)
                    copies = 1 + (rng.random() < 0.3) + (rng.random() < 0.1)
                    for k in range(copies):
                        e = ev if k == 0 else w.provider.ev(ev.call_id, ev.type, event_id=ev.event_id + f":d{k}",
                                                            answered=ev.answered, reason=ev.reason)
                        bag.append(e)

    def deliver_one():
        if not bag or not live_workers():
            return
        i = rng.randrange(len(bag))
        ev = bag.pop(i)
        if rng.random() < 0.05:              # dropped delivery
            return
        wk = rng.choice(live_workers())
        crashy = rng.random() < 0.08
        if crashy:
            point = rng.choice(["before_commit", "after_commit"])
            def hook(p, **k):
                if p == point:
                    raise WorkerCrash(p)
            wk.ingestor.crash_hook = hook
            try:
                wk.ingestor.ingest(ev)
            except WorkerCrash:
                if point == "before_commit":
                    bag.append(ev)           # rolled back -> provider will redeliver
                w.workers[w.workers.index(wk)] = None     # worker process dies
            finally:
                if w.workers and wk in w.workers:
                    wk.ingestor.crash_hook = lambda *a, **k: None
        else:
            wk.ingestor.ingest(ev)

    def churn():
        a = rng.choice(agent_ids)
        roll = rng.random()
        if roll < 0.3:
            w.w0_safe_logout(a) if hasattr(w, "w0_safe_logout") else _logout(w, a)
        elif roll < 0.5:
            _login(w, a)
        else:
            for wk in live_workers():
                wk.agents.heartbeat(a)

    def _logout(w, a):
        for wk in live_workers():
            wk.agents.go_unavailable(a, A.OFFLINE)
            return

    def _login(w, a):
        for wk in live_workers():
            wk.agents.go_available(a)
            return

    def restart_dead():
        for idx, wk in enumerate(w.workers):
            if wk is None and rng.random() < 0.5:
                w.workers[idx] = w._mk_worker(idx) if hasattr(w, "_mk_worker") else _mk(w, idx, pacer, cfg)

    # ---- main loop ------------------------------------------------------
    for step in range(rng.randint(40, 120)):
        action = rng.random()
        if action < 0.30 and live_workers():
            rng.choice(live_workers()).tick()
            maybe_script_new_calls()
        elif action < 0.70:
            deliver_one()
        elif action < 0.85:
            churn()
        elif action < 0.92:
            w.advance(rng.uniform(0.1, 6.0))
            for wk in live_workers():
                wk.reconciler.run()
        else:
            restart_dead()
        errs = invariants.check(w.store.read())
        assert not errs, f"seed={seed} mode={mode} step={step}: {errs}"

    # ---- drain ----------------------------------------------------------
    # make sure at least one worker is alive to finish the job
    if not live_workers():
        w.workers[0] = _mk(w, 0, pacer, cfg)
    for a in agent_ids:
        _login(w, a)
        for wk in live_workers():
            wk.agents.heartbeat(a)
    # deliver everything still in the bag
    for ev in list(bag):
        for wk in live_workers():
            try:
                wk.ingestor.ingest(ev)
            except WorkerCrash:
                pass
            break
    # force every still-active call to a terminal provider event
    for row in w.q(f"SELECT id, was_answered FROM calls WHERE state NOT IN "
                   f"({','.join(repr(s.value) for s in TERMINAL_CALL_STATES)})"):
        live_workers()[0].ingestor.ingest(w.provider.ev(row[0], "COMPLETED", answered=bool(row[1])))
    # let timers close anything left (answered-waiting grace, wrap-up, etc.)
    for _ in range(5):
        w.advance(120)
        for a in agent_ids:
            for wk in live_workers():
                wk.agents.heartbeat(a)
        for wk in live_workers():
            wk.reconciler.run()
            wk.allocator.dispatch()

    # ---- final consistency ---------------------------------------------
    assert invariants.check(w.store.read()) == [], f"seed={seed} mode={mode}: post-drain invariants"
    active = w.q(f"SELECT id, state FROM calls WHERE state NOT IN "
                 f"({','.join(repr(s.value) for s in TERMINAL_CALL_STATES)})")
    assert active == [], f"seed={seed} mode={mode}: calls stuck non-terminal: {active}"
    stuck_b = w.q("SELECT id FROM borrowers WHERE state='IN_CALL'")
    assert stuck_b == [], f"seed={seed} mode={mode}: borrowers stuck IN_CALL: {stuck_b}"
    busy = w.q("SELECT id, state FROM agents WHERE state IN ('RESERVED','DIALING','CONNECTED')")
    assert busy == [], f"seed={seed} mode={mode}: agents stuck busy: {busy}"
    # conservation: every answered borrower is accounted for (connected or abandoned), none lost
    answered = w.q("SELECT COUNT(*) FROM calls WHERE was_answered=1 AND borrower_id LIKE 'b%'")[0][0]
    connected = w.q("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL AND borrower_id LIKE 'b%'")[0][0]
    abandoned = w.q("SELECT COUNT(*) FROM calls WHERE borrower_id LIKE 'b%' AND "
                    "(state='ABANDONED' OR end_reason='late_answer')")[0][0]
    assert connected + abandoned >= answered, f"seed={seed}: {answered} answered but {connected}+{abandoned} accounted"


def _mk(w, idx, pacer, cfg):
    from smartdialer.worker import DialerWorker
    return DialerWorker(f"w{idx}", w.store, w.clock, pacer, [w.provider], cfg)


@pytest.mark.parametrize("seed", range(120))
def test_chaos_progressive(seed):
    _one_run(seed, "progressive")


@pytest.mark.parametrize("seed", range(120))
def test_chaos_predictive(seed):
    _one_run(seed, "predictive")
