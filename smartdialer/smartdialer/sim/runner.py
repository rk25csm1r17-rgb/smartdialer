"""Discrete-event simulation harness: agents, borrowers, providers, N workers,
scripted disruptions, metrics. Everything runs on simulated time against the
real dialer code and a real SQLite database (in memory)."""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from typing import Callable

from .. import invariants
from ..agents import AgentService
from ..allocator import WorkerCrash
from ..clock import SimClock
from ..config import DialerConfig
from ..pacing import PredictivePacer, ProgressivePacer
from ..providers.mock import PROFILE_A, PROFILE_B, MockProvider, Population, ProviderProfile
from ..safety import SafetyConfig
from ..states import AgentState as A
from ..store import Store
from ..worker import DialerWorker
from .engine import Scheduler


@dataclass
class Scenario:
    name: str
    mode: str = "predictive"                       # predictive | progressive
    agents: int = 50
    duration_s: float = 1800
    answer_rate: Callable[[float], float] | float = 0.3
    talk_time_s: Callable[[float], float] | float = 120.0
    providers: tuple = ("A",)                      # preference order
    workers: int = 3
    borrowers: int = 30000
    seed: int = 1
    tick_s: float = 1.0
    profile_overrides: dict = field(default_factory=dict)
    # scripted disruptions: list of (time, kind, arg)
    #   ("outage", "provider_a", seconds) | ("drop_agents", n, over_seconds)
    #   ("add_agents", n, None) | ("crash_worker", worker_index, restart_after_s or None)
    script: list = field(default_factory=list)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    dialer: DialerConfig = field(default_factory=DialerConfig)
    description: str = ""


class Simulation:
    def __init__(self, sc: Scenario, verbose: bool = False):
        self.sc, self.verbose = sc, verbose
        self.clock = SimClock(0.0)
        self.sched = Scheduler(self.clock)
        self.store = Store(":memory:")
        self.rng = random.Random(sc.seed)
        self.pop = Population(answer_rate=sc.answer_rate, talk_time_s=sc.talk_time_s)
        profiles = {"A": PROFILE_A, "B": PROFILE_B}
        self.providers = []
        for i, key in enumerate(sc.providers):
            prof = profiles[key]
            if key in sc.profile_overrides:
                prof = ProviderProfile(**{**prof.__dict__, **sc.profile_overrides[key]})
            p = MockProvider(prof, self.sched, self.pop, seed=sc.seed * 100 + i)
            p.set_event_sink(self._webhook)
            self.providers.append(p)
        self.workers: list[DialerWorker | None] = []
        self._incarnation = 0
        for i in range(sc.workers):
            self.workers.append(self._make_worker(i))
        self.agent_svc = AgentService(self.store, self.clock, sc.dialer)
        self.online: set[str] = set()
        self.timeline: list[dict] = []
        self.log: list[str] = []
        self.invariant_errors: list[str] = []
        self.agent_seconds = {s.value: 0.0 for s in A}
        self._setup()

    # ------------------------------------------------------------------ setup
    def _make_worker(self, i: int) -> DialerWorker:
        self._incarnation += 1
        sc = self.sc
        pacer = (PredictivePacer(wrap_up_s=sc.dialer.wrap_up_s) if sc.mode == "predictive" else ProgressivePacer())
        return DialerWorker(f"w{i}", self.store, self.clock, pacer, self.providers, sc.dialer, sc.safety,
                            seed=sc.seed * 1000 + self._incarnation)

    def _setup(self):
        sc = self.sc
        with self.store.tx() as c:
            c.executemany("INSERT INTO borrowers(id, phone, state, priority, next_attempt_at) VALUES (?,?,?,?,0)",
                          [(f"b{i:06d}", f"+9100000{i:06d}", "READY", 0) for i in range(sc.borrowers)])
        for i in range(sc.agents):
            self._agent_online(f"a{i:05d}")
        for i in range(sc.workers):
            off = i * sc.tick_s / sc.workers
            self.sched.every(sc.tick_s, (lambda i=i: self._tick_worker(i)), start=0.5 + off, until=sc.duration_s)
        self.sched.every(5.0, self._heartbeats, start=1.0, until=sc.duration_s)
        self.sched.every(1.0, self._sample, start=0.0, until=sc.duration_s)
        self.sched.every(60.0, self._minute, start=60.0, until=sc.duration_s)
        self.sched.every(10.0, self._check, start=5.0, until=sc.duration_s)
        for t, kind, *args in sc.script:
            self.sched.at(t, lambda kind=kind, args=args: self._disrupt(kind, *args))

    def _agent_online(self, aid):
        self.agent_svc.add(aid)
        self.agent_svc.go_available(aid)
        self.online.add(aid)

    def _webhook(self, ev):
        alive = [w for w in self.workers if w is not None]
        if not alive:     # nobody to receive it: provider retries later
            self.sched.after(2.0, lambda: self._webhook(ev))
            return
        w = self.rng.choice(alive)
        try:
            w.ingestor.ingest(ev)
        except WorkerCrash:
            self.workers[self.workers.index(w)] = None
            self._say(f"worker {w.id} CRASHED while handling {ev.type}; provider will redeliver")
            self.sched.after(1.0, lambda: self._webhook(ev))

    def _tick_worker(self, i):
        w = self.workers[i]
        if w is not None:
            try:
                w.tick()
            except WorkerCrash as e:
                self.workers[i] = None
                self._say(f"worker w{i} CRASHED at {e}")

    def _heartbeats(self):
        for a in self.online:
            self.agent_svc.heartbeat(a)

    # ------------------------------------------------------------------ disruptions
    def _say(self, msg):
        line = f"[t={self.clock.now():7.1f}s] {msg}"
        self.log.append(line)
        if self.verbose:
            print(line)

    def _disrupt(self, kind, *args):
        if kind == "outage":
            name, secs = args
            p = next(p for p in self.providers if p.name == name)
            p.set_outage(True)
            self._say(f"OUTAGE start on {name} for {secs}s")

            def end():
                p.set_outage(False)
                self._say(f"OUTAGE end on {name}")
            self.sched.after(secs, end)
        elif kind == "drop_agents":
            n, over = args
            victims = sorted(self.online)[:n]
            self._say(f"{n} agents start dropping over {over}s")
            for j, a in enumerate(victims):
                def go(a=a):
                    self.online.discard(a)
                    r = self.agent_svc.go_unavailable(a, A.OFFLINE)
                    if "call" in r:
                        self._say(f"agent {a} dropped: {r}")
                self.sched.after(over * j / max(1, n), go)
        elif kind == "add_agents":
            n = args[0]
            base = len(self.online) + 100000
            for j in range(n):
                self._agent_online(f"a{base + j:06d}")
            self._say(f"{n} agents joined")
        elif kind == "crash_worker":
            idx, restart_after = args
            self.workers[idx] = None
            self._say(f"worker w{idx} CRASHED (in-memory state lost, leases left behind)")
            if restart_after is not None:
                def restart():
                    self.workers[idx] = self._make_worker(idx)
                    self._say(f"worker w{idx} restarted")
                self.sched.after(restart_after, restart)

    # ------------------------------------------------------------------ metrics
    def _sample(self):
        c = self.store.read()
        for s, n in c.execute("SELECT state, COUNT(*) FROM agents GROUP BY state"):
            self.agent_seconds[s] += n

    def _check(self):
        errs = invariants.check(self.store.read())
        if errs:
            self.invariant_errors.extend(f"t={self.clock.now():.0f}: {e}" for e in errs)

    def _minute(self):
        c = self.store.read()
        t1 = self.clock.now()
        t0 = t1 - 60
        one = lambda sql, *a: c.execute(sql, a).fetchone()[0]
        states = {r[0]: r[1] for r in c.execute("SELECT state, COUNT(*) FROM agents GROUP BY state")}
        dec = c.execute("""SELECT action, COUNT(*), SUM(requested), SUM(approved) FROM safety_decisions
                           WHERE ts > ? AND ts <= ? GROUP BY action""", (t0, t1)).fetchall()
        row = {
            "t_min": round(t1 / 60),
            "agents_logged_in": sum(v for k, v in states.items() if k != "OFFLINE"),
            "connected_now": states.get("CONNECTED", 0),
            "available_now": states.get("AVAILABLE", 0),
            "initiated": one("SELECT COUNT(*) FROM calls WHERE initiated_at > ? AND initiated_at <= ?", t0, t1),
            "answered": one("SELECT COUNT(*) FROM calls WHERE answered_at > ? AND answered_at <= ?", t0, t1),
            "connected": one("SELECT COUNT(*) FROM calls WHERE connected_at > ? AND connected_at <= ?", t0, t1),
            "abandoned": one("""SELECT COUNT(*) FROM calls WHERE ended_at > ? AND ended_at <= ?
                                AND (state='ABANDONED' OR end_reason='late_answer')""", t0, t1),
            "failed_system": one("""SELECT COUNT(*) FROM calls WHERE ended_at > ? AND ended_at <= ? AND state='FAILED'
                                    AND end_reason IN ('provider_error','provider_timeout','setup_timeout','no_provider')""", t0, t1),
            "requested": sum(r[2] or 0 for r in dec),
            "approved": sum(r[3] or 0 for r in dec),
            "decisions": {r[0]: r[1] for r in dec if r[0] != "APPROVE" or True},
            "health": "/".join(f"{n[-1]}:{b.current()[0]}" for w in self.workers if w for n, b in w.router.breakers.items())[:23],
        }
        busy = c.execute("SELECT COUNT(*) FROM agents WHERE state='CONNECTED'").fetchone()[0]
        row["util_now"] = round(busy / row["agents_logged_in"], 2) if row["agents_logged_in"] else 0
        self.timeline.append(row)

    # ------------------------------------------------------------------ run
    def run(self) -> dict:
        self.sched.run_until(self.sc.duration_s)
        # drain: let in-flight events land so the end state is consistent
        return self.summary()

    def summary(self) -> dict:
        c = self.store.read()
        one = lambda sql, *a: c.execute(sql, a).fetchone()[0]
        logged = sum(v for k, v in self.agent_seconds.items() if k != "OFFLINE")
        answered = one("SELECT COUNT(*) FROM calls WHERE was_answered=1")
        abandoned = one("SELECT COUNT(*) FROM calls WHERE state='ABANDONED' OR end_reason='late_answer'")
        dec = {r[0]: {"count": r[1], "requested": r[2], "approved": r[3]} for r in c.execute(
            "SELECT action, COUNT(*), SUM(requested), SUM(approved) FROM safety_decisions GROUP BY action")}
        ends = {f"{r[0]}:{r[1]}": r[2] for r in c.execute(
            "SELECT state, end_reason, COUNT(*) FROM calls WHERE ended_at IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC")}
        examples = {}
        for action in ("APPROVE", "REDUCE", "REJECT", "FALLBACK_PROGRESSIVE"):
            r = c.execute("""SELECT ts, worker, requested, approved, detail FROM safety_decisions WHERE action=?
                             ORDER BY ABS(ts - ?) LIMIT 1""", (action, self.sc.duration_s / 2)).fetchone()
            if r:
                examples[action] = {"t": round(r[0], 1), "worker": r[1], "requested": r[2], "approved": r[3],
                                    **json.loads(r[4])}
        w_actions, w_effects, shed = {}, {}, 0
        for w in self.workers:
            if w is None:
                continue
            for k, v in w.reconciler.actions.items():
                w_actions[k] = w_actions.get(k, 0) + v
            for k, v in w.ingestor.effects.items():
                w_effects[k] = w_effects.get(k, 0) + v
            shed += w.allocator.stats["shed"]
        return {
            "scenario": self.sc.name,
            "mode": self.sc.mode,
            "agents": self.sc.agents,
            "duration_min": self.sc.duration_s / 60,
            "talk_utilization": round(self.agent_seconds["CONNECTED"] / logged, 3) if logged else 0,
            "occupancy_incl_wrap": round((self.agent_seconds["CONNECTED"] + self.agent_seconds["WRAP_UP"]) / logged, 3) if logged else 0,
            "idle_share": round(self.agent_seconds["AVAILABLE"] / logged, 3) if logged else 0,
            "calls_created": one("SELECT COUNT(*) FROM calls"),
            "calls_initiated": one("SELECT COUNT(*) FROM calls WHERE initiated_at IS NOT NULL OR state IN ('RINGING','ANSWERED','CONNECTED','COMPLETED','ABANDONED')"),
            "calls_answered": answered,
            "calls_connected": one("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL"),
            "abandoned": abandoned,
            "abandon_rate": round(abandoned / answered, 4) if answered else 0.0,
            "safety_decisions": dec,
            "call_outcomes": ends,
            "reconciler_actions": w_actions,
            "event_effects": w_effects,
            "shed_calls": shed,
            "provider_stats": {p.name: p.stats for p in self.providers},
            "decision_examples": examples,
            "invariant_violations": len(self.invariant_errors),
            "invariant_examples": self.invariant_errors[:5],
        }
