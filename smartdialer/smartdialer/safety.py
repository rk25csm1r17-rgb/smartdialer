"""Safety Controller: the only component allowed to authorise new calls.

It sits between pacing and the allocator and is invoked *by the allocator inside
the allocation write transaction*:

    Pacer.propose() --(number)--> CallAllocator.allocate()
                                     BEGIN IMMEDIATE
                                       SafetyController.authorize()  <- reads its OWN snapshot
                                       claim borrowers/agents, insert calls (= the permit)
                                     COMMIT

Why that makes it non-bypassable:
  * The pacer has no reference to the allocator, provider, or this controller.
    Its only output is an integer that is treated as a *request*.
  * The only code that inserts rows into `calls` is the allocator, and it only
    inserts as many as the Decision returned here allowed.
  * Provider dials happen only for rows in `calls` (via the outbox), and a row
    that isn't dialled within `permit_ttl_s` is cancelled, so an old approval
    can't be cashed in later against changed capacity.
  * Because the decision and the inserts share one serialised transaction, two
    workers cannot both spend the same headroom.
  * SafetyConfig is frozen and owned by this object; nothing on the pacing path
    can change it.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from typing import Callable

from .pacing import PacingProposal
from .snapshot import Snapshot, take_snapshot


@dataclass(frozen=True)
class SafetyConfig:
    max_abandon_rate: float = 0.03          # regulatory ceiling (e.g. 3% over the window)
    warn_fraction: float = 0.5              # above 50% of ceiling: halve the risk budget
    fallback_fraction: float = 0.8          # above 80% of ceiling: progressive only
    abandon_min_answered: int = 20
    risk_epsilon: float = 0.02              # max P(more answers than agents) per decision
    min_samples: int = 50                   # resolved attempts needed before predictive is allowed
    drift_z: float = 3.0                    # short-vs-long answer-rate jump that forces fallback
    max_dial_ratio: float = 3.0             # hard cap: unbound calls <= ratio * capacity
    max_calls_per_decision: int = 200
    ucb_z: float = 1.645                    # one-sided 95% upper bound on answer rate
    default_min_time_to_answer_s: float = 3.0
    shed_ringing: bool = True               # may cancel ringing (unanswered) calls when capacity collapses
    degraded_max_calls: int = 2             # provider HALF_OPEN: only a trickle of probe calls
    shed_epsilon: float = 0.05              # shed when overflow risk is clearly out of bounds (> 2.5x the dial budget)
    answer_grace_s: float = 2.0             # must match DialerConfig.abandon_grace_s


@dataclass(frozen=True)
class Decision:
    requested: int
    approved: int
    mode: str              # mode the approved calls must be placed in
    action: str            # APPROVE | REDUCE | REJECT | FALLBACK_PROGRESSIVE
    reasons: tuple
    detail: dict = field(default_factory=dict)
    id: int | None = None


def wilson_upper(k: int, n: int, z: float) -> float:
    if n == 0:
        return 1.0
    ph = k / n
    denom = 1 + z * z / n
    centre = ph + z * z / (2 * n)
    margin = z * math.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n))
    return min(1.0, (centre + margin) / denom)


def binom_tail_gt(n: int, p: float, c: int) -> float:
    """P(X > c) for X ~ Binomial(n, p)."""
    if c < 0:
        return 1.0
    if c >= n:
        return 0.0
    if p <= 0:
        return 0.0
    if p >= 1:
        return 1.0
    lp, lq = math.log(p), math.log1p(-p)
    lg = math.lgamma
    cdf = 0.0
    for k in range(0, c + 1):
        cdf += math.exp(lg(n + 1) - lg(k + 1) - lg(n - k + 1) + k * lp + (n - k) * lq)
    return max(0.0, 1.0 - cdf)


def max_calls_within_risk(capacity: int, p: float, eps: float, hard_cap: int) -> int:
    """Largest N <= hard_cap with P(Binomial(N, p) > capacity) <= eps. Monotone in N -> binary search."""
    if capacity <= 0:
        return 0
    lo, hi = capacity, max(capacity, hard_cap)     # N = capacity is always safe
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if binom_tail_gt(mid, p, capacity) <= eps:
            lo = mid
        else:
            hi = mid - 1
    return lo


class OverflowRisk:
    """Distribution of 'how many in-flight unbound calls will be answered'.
    Calls are independent Bernoullis with their own probabilities (Poisson-binomial).

    Exact mode (capacity <= EXACT_MAX): dist[k] = P(exactly k answers) for k <= capacity
    plus the overflow mass P(answers > capacity); adding a call is O(capacity).
    Large pools: normal approximation with continuity correction; adding a call is O(1).
    With hundreds of free agents the variance is large and the CLT error is far below
    the epsilons we use, while the exact DP would cost O(in-flight x capacity) per
    decision (15 s at 10,000 agents in scripts/loadtest.py)."""
    EXACT_MAX = 400

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.exact = capacity <= self.EXACT_MAX
        self.dist = ([1.0] + [0.0] * max(capacity, 0)) if self.exact else None
        self._over = 0.0 if capacity >= 0 else 1.0
        self.mean = 0.0
        self.var = 0.0
        self.n = 0

    def add(self, p: float) -> None:
        self.n += 1
        self.mean += p
        self.var += p * (1 - p)
        if not self.exact or self.capacity < 0:
            return
        d, q = self.dist, 1.0 - p
        self._over += d[-1] * p
        for k in range(len(d) - 1, 0, -1):
            d[k] = d[k] * q + d[k - 1] * p
        d[0] *= q

    @property
    def over(self) -> float:
        if self.exact or self.capacity < 0:
            return self._over
        if self.var <= 0:
            return 1.0 if self.mean > self.capacity else 0.0
        z = (self.capacity + 0.5 - self.mean) / math.sqrt(self.var)
        return 0.5 * math.erfc(z / math.sqrt(2))

    def copy(self) -> "OverflowRisk":
        o = OverflowRisk.__new__(OverflowRisk)
        o.__dict__.update(self.__dict__)
        if self.dist is not None:
            o.dist = list(self.dist)
        return o


class SafetyController:
    __slots__ = ("_cfg", "_health")

    def __init__(self, cfg: SafetyConfig, provider_health: Callable[[], str]):
        self._cfg = cfg
        self._health = provider_health

    @property
    def cfg(self) -> SafetyConfig:
        return self._cfg

    # -------------------------------------------------------------- shared numbers
    def _risk_view(self, s: Snapshot) -> dict:
        cfg = self._cfg
        ucb_s = wilson_upper(s.short.answered, s.short.attempts, cfg.ucb_z) if s.short.attempts >= 10 else None
        ucb_l = wilson_upper(s.long.answered, s.long.attempts, cfg.ucb_z) if s.long.attempts else 1.0
        p_safe = max(x for x in (ucb_s, ucb_l) if x is not None)
        min_tta = s.min_time_to_answer_s or cfg.default_min_time_to_answer_s
        # Wrap-up is system-timed, so its end is a fact, not a forecast. An agent whose
        # wrap-up ends before the earliest plausible answer (+ the grace period an
        # answered borrower may wait) is real capacity for calls started now.
        wrap_credit = sum(1 for w in s.wrap_until if w <= s.now + min_tta + cfg.answer_grace_s)
        capacity = s.available - s.answered_waiting + wrap_credit
        fallback = []
        health = self._health()
        if health == "DEGRADED":
            fallback.append("provider_degraded")
        if s.long.attempts < cfg.min_samples:
            fallback.append(f"insufficient_samples({s.long.attempts}<{cfg.min_samples})")
        ab = s.abandon_rate if s.abandon_answered >= cfg.abandon_min_answered else None
        if ab is not None and ab >= cfg.fallback_fraction * cfg.max_abandon_rate:
            fallback.append(f"abandon_rate_high({ab:.3f})")
        if s.answered_waiting > 0:
            fallback.append(f"answered_waiting({s.answered_waiting})")
        z = None
        if s.short.attempts >= 20 and s.long.attempts >= 50:
            p1, p2 = s.short.rate, s.long.rate
            pool = (s.short.answered + s.long.answered) / (s.short.attempts + s.long.attempts)
            se = math.sqrt(max(pool * (1 - pool), 1e-9) * (1 / s.short.attempts + 1 / s.long.attempts))
            z = (p1 - p2) / se
            if z > cfg.drift_z:
                fallback.append(f"answer_rate_rising(z={z:.1f})")
        eps = cfg.risk_epsilon * (0.5 if ab is not None and ab >= cfg.warn_fraction * cfg.max_abandon_rate else 1.0)
        return dict(health=health, p_safe=round(p_safe, 3), ucb_short=ucb_s and round(ucb_s, 3),
                    ucb_long=round(ucb_l, 3), capacity=capacity, wrap_credit=wrap_credit,
                    abandon_rate=ab, eps=eps, drift_z=z and round(z, 2), fallback=fallback)

    # -------------------------------------------------------------- authorise
    def authorize(self, c, proposal: PacingProposal, now: float, worker: str) -> Decision:
        """Must be called inside the allocation transaction (`c`)."""
        cfg = self._cfg
        s = take_snapshot(c, now)
        req = max(0, min(int(proposal.n), cfg.max_calls_per_decision))
        r = self._risk_view(s)
        prog_capacity = max(0, s.available - s.answered_waiting - s.unbound_inflight)
        detail = {"available": s.available, "answered_waiting": s.answered_waiting,
                  "unbound_inflight": s.unbound_inflight, "prog_capacity": prog_capacity, **r}

        def done(approved, mode, action, reasons):
            d = Decision(req, approved, mode, action, tuple(reasons), detail)
            if req == 0:
                return d          # nothing asked: don't log a row per idle tick
            cur = c.execute("""INSERT INTO safety_decisions(ts, worker, mode, requested, approved, action, detail)
                               VALUES (?,?,?,?,?,?,?)""",
                            (now, worker, mode, req, approved, action,
                             json.dumps({"reasons": reasons, **detail, "pacer": proposal.explain}, default=str)))
            return Decision(**{**asdict(d), "id": cur.lastrowid})

        if r["health"] == "DOWN":
            return done(0, proposal.mode, "REJECT", ["all_providers_down"])

        if r["health"] == "DEGRADED":
            # Recovering provider: progressive AND throttled, so a relapse costs a couple of calls, not 40.
            ap = min(req, prog_capacity, cfg.degraded_max_calls)
            return done(ap, "progressive", "FALLBACK_PROGRESSIVE" if proposal.mode == "predictive" else _action(req, ap),
                        ["provider_degraded_probe_only"])

        if proposal.mode == "progressive":
            ap = min(req, prog_capacity)
            return done(ap, "progressive", _action(req, ap), ["progressive_capacity"])

        if r["fallback"]:
            ap = min(req, prog_capacity)
            return done(ap, "progressive", "FALLBACK_PROGRESSIVE", r["fallback"])

        hard_cap = int(cfg.max_dial_ratio * max(r["capacity"], 0))
        risk = self._inflight_risk(s, r)
        allowed, stop = 0, None
        while allowed < req:
            trial = risk.copy()
            trial.add(r["p_safe"])
            if trial.over > r["eps"]:
                stop = f"risk_bound(P(answers>{r['capacity']}) would exceed {r['eps']} at p_new={r['p_safe']})"
                break
            if trial.n > hard_cap:
                stop = f"hard_cap({hard_cap} = ratio {cfg.max_dial_ratio} x capacity {r['capacity']})"
                break
            risk, allowed = trial, allowed + 1
        detail.update(inflight_overflow_p=round(self._inflight_risk(s, r).over, 4),
                      overflow_p_after=round(risk.over, 4), allowed_new=allowed, hard_cap=hard_cap)
        ap = allowed
        reasons = [stop] if stop else []
        return done(ap, "predictive", _action(req, ap), reasons)

    def _inflight_risk(self, s: Snapshot, r: dict, p_override: float | None = None, ages=None) -> OverflowRisk:
        risk = OverflowRisk(r["capacity"])
        for age in (s.unbound_ages if ages is None else ages):
            p = p_override if p_override is not None else s.p_answer_given_age(r["p_safe"], age)
            risk.add(p)
        return risk

    # -------------------------------------------------------------- load shedding
    def shed_target(self, c, now: float) -> tuple[int, dict]:
        """How many of the NEWEST not-yet-answered unbound calls must be cancelled
        right now because capacity dropped (agents vanished) below what's in flight."""
        s = take_snapshot(c, now)
        if s.unbound_inflight == 0:
            return 0, {}
        r = self._risk_view(s)
        eps = self._cfg.shed_epsilon
        # Under any fallback condition we stop trusting the statistics: worst case p = 1.
        p_override = 1.0 if r["fallback"] else None
        risk = OverflowRisk(r["capacity"])
        keep = 0
        for age in s.unbound_ages:                       # oldest first: they have the lowest p
            p = p_override if p_override is not None else s.p_answer_given_age(r["p_safe"], age)
            risk.add(p)
            if risk.over > eps:
                break
            keep += 1
        excess = s.unbound_inflight - keep
        return excess, {"capacity": r["capacity"], "keep": keep, "inflight": s.unbound_inflight}


def _action(req: int, ap: int) -> str:
    if ap == req:
        return "APPROVE"
    return "REJECT" if ap == 0 else "REDUCE"
