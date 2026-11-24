"""Pacing engines. They PROPOSE how many calls to start. They never touch the
database for writes, never see the Safety Controller, and have no reference to
the allocator or any provider: a proposal is just a number plus an explanation.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .snapshot import Snapshot


@dataclass(frozen=True)
class PacingProposal:
    mode: str              # "progressive" | "predictive"
    n: int
    explain: dict = field(default_factory=dict)


class ProgressivePacer:
    """One free agent -> one agent-bound call."""
    mode = "progressive"

    def propose(self, s: Snapshot) -> PacingProposal:
        free = s.available - s.answered_waiting - s.unbound_inflight
        return PacingProposal("progressive", max(0, free), {"free_agents": free})


@dataclass
class PredictivePacer:
    """Forecast how many agents will be free when calls started *now* get answered,
    and dial enough calls that the expected number of answers matches that.

        H            = average time from dial to answer (observed)
        expected_free = available now
                      + agents whose (system-timed) wrap-up ends within H
                      + sum over live conversations of P(talk ends and wrap-up done within H)
                        (memoryless talk-time assumption: 1 - exp(-(H - wrap) / avg_talk))
        need         = expected_free - expected answers from unbound calls already in flight
                       (each weighted by P(answer | still ringing at its age))
        n            = need / p * aggressiveness
    where p is the recent answer rate and `aggressiveness` is a feedback term
    that backs off when the observed abandon rate approaches the target.
    """
    wrap_up_s: float = 10.0
    prior_answer_rate: float = 0.3
    prior_talk_s: float = 120.0
    prior_time_to_answer_s: float = 12.0
    target_abandon_rate: float = 0.01
    max_dial_ratio: float = 5.0
    mode: str = "predictive"

    def propose(self, s: Snapshot) -> PacingProposal:
        if s.short.attempts >= 30:
            p, p_src = s.short.rate, "short_window"
        elif s.long.attempts >= 30:
            p, p_src = s.long.rate, "long_window"
        else:
            p, p_src = self.prior_answer_rate, "prior"
        p = min(max(p, 0.02), 1.0)
        H = s.avg_time_to_answer_s or self.prior_time_to_answer_s
        talk = s.avg_talk_s or self.prior_talk_s

        free_now = s.available - s.answered_waiting
        wrap_free = sum(1 for w in s.wrap_until if w - s.now <= H)
        if H > self.wrap_up_s:
            p_end = 1 - math.exp(-(H - self.wrap_up_s) / talk)
            talk_free = p_end * len(s.connected_elapsed)
        else:
            p_end, talk_free = 0.0, 0.0
        expected_free = free_now + wrap_free + talk_free
        # Calls that have already rung a while are less likely to answer (see Snapshot).
        inflight_answers = sum(s.p_answer_given_age(p, a) for a in s.unbound_ages)
        need = expected_free - inflight_answers

        ab = s.abandon_rate if s.abandon_answered >= 20 else None
        if ab is None:
            aggr = 1.0
        else:
            # proportional back-off: at target -> 1.0, at 2x target -> 0.5, floor 0.3
            aggr = max(0.3, min(1.1, 1.0 - 0.5 * (ab - self.target_abandon_rate) / self.target_abandon_rate))

        raw = need / p if need > 0 else 0.0
        cap = max(0.0, self.max_dial_ratio * max(expected_free, 0) - s.unbound_inflight)
        n = int(max(0, min(math.floor(raw * aggr), cap)))
        return PacingProposal("predictive", n, {
            "p_answer": round(p, 3), "p_source": p_src, "horizon_s": round(H, 1), "avg_talk_s": round(talk, 1),
            "free_now": free_now, "wrap_free": wrap_free, "talk_free": round(talk_free, 2),
            "expected_free": round(expected_free, 2), "unbound_inflight": s.unbound_inflight,
            "inflight_expected_answers": round(inflight_answers, 2), "need": round(need, 2),
            "abandon_rate": None if ab is None else round(ab, 4), "aggressiveness": round(aggr, 2),
            "raw": round(raw, 2), "cap": round(cap, 1), "n": n,
        })
