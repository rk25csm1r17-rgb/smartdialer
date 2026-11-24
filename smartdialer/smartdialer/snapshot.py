"""A point-in-time view of the campaign, built only from the database.

The pacing engine reads one outside any transaction (it may be slightly stale;
that's fine, it only *proposes*). The Safety Controller reads its own inside the
allocation write transaction, so its numbers cannot change before the calls it
authorises are written.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Window:
    attempts: int = 0     # resolved dial outcomes (answered or genuinely unanswered)
    answered: int = 0

    @property
    def rate(self) -> float | None:
        return self.answered / self.attempts if self.attempts else None


@dataclass
class Snapshot:
    now: float
    agent_counts: dict
    available: int                    # AVAILABLE agents, not bound to anything
    wrap_until: list                  # wrap-up end times (system-timed => deterministic)
    connected_elapsed: list           # seconds into each live conversation
    unbound_inflight: int             # predictive calls not yet answered (RESERVED/INITIATED/RINGING)
    unbound_ages: list                # seconds since creation for each of those, oldest first
    bound_inflight: int               # progressive calls not yet answered
    answered_waiting: int             # answered, no agent yet: the dangerous ones
    short: Window                     # last SHORT_S seconds
    long: Window                      # last LONG_S seconds
    abandon_answered: int             # answered calls in abandon window
    abandon_count: int
    avg_talk_s: float | None
    avg_time_to_answer_s: float | None
    min_time_to_answer_s: float | None
    tta_samples: list = field(default_factory=list)   # sorted dial->answer times, answered calls only

    @property
    def logged_in(self) -> int:
        return sum(v for k, v in self.agent_counts.items() if k not in ("OFFLINE",))

    def p_answer_given_age(self, p: float, age: float) -> float:
        """P(call eventually answers | it has not answered after `age` seconds).

        Bayes with the empirical dial->answer time distribution S(a) = P(tta > a | answers):
            p * S(a) / (p * S(a) + (1 - p))
        A call that has rung for 25s when nearly everyone who answers does so within
        15s is very unlikely to answer: counting it at full p wastes capacity, which is
        exactly where a lot of predictive's advantage comes from. S is smoothed
        (+1 / n+1) so it never claims certainty, and with too few samples we return p."""
        n = len(self.tta_samples)
        if n < 30 or age <= 0:
            return p
        import bisect
        surv = (n - bisect.bisect_right(self.tta_samples, age) + 1) / (n + 1)
        return p * surv / (p * surv + (1 - p))

    @property
    def abandon_rate(self) -> float | None:
        return self.abandon_count / self.abandon_answered if self.abandon_answered else None


SHORT_S = 120.0
LONG_S = 900.0
ABANDON_WINDOW_S = 1800.0
_REAL_OUTCOME = "('no_answer','busy','ring_timeout')"


def _window(c, now: float, span: float) -> Window:
    t0 = now - span
    ans = c.execute("SELECT COUNT(*) FROM calls WHERE answered_at > ?", (t0,)).fetchone()[0]
    unans = c.execute(f"""SELECT COUNT(*) FROM calls WHERE ended_at > ? AND was_answered=0
                          AND end_reason IN {_REAL_OUTCOME}""", (t0,)).fetchone()[0]
    return Window(attempts=ans + unans, answered=ans)


def take_snapshot(c, now: float) -> Snapshot:
    counts = {r[0]: r[1] for r in c.execute("SELECT state, COUNT(*) FROM agents GROUP BY state")}
    wraps = [r[0] for r in c.execute("SELECT wrap_until FROM agents WHERE state='WRAP_UP' AND wrap_until IS NOT NULL")]
    conn_el = [now - r[0] for r in c.execute(
        "SELECT connected_at FROM calls WHERE state='CONNECTED' AND connected_at IS NOT NULL")]
    inflight = c.execute("""SELECT SUM(agent_id IS NULL), SUM(agent_id IS NOT NULL) FROM calls
                            WHERE state IN ('RESERVED','INITIATED','RINGING')""").fetchone()
    ages = [now - r[0] for r in c.execute("""SELECT created_at FROM calls WHERE agent_id IS NULL
                                              AND state IN ('RESERVED','INITIATED','RINGING') ORDER BY created_at""")]
    waiting = c.execute("SELECT COUNT(*) FROM calls WHERE state='ANSWERED' AND agent_id IS NULL").fetchone()[0]
    t_ab = now - ABANDON_WINDOW_S
    ab_ans = c.execute("SELECT COUNT(*) FROM calls WHERE answered_at > ?", (t_ab,)).fetchone()[0]
    ab_cnt = c.execute("""SELECT COUNT(*) FROM calls WHERE answered_at > ?
                          AND (state='ABANDONED' OR end_reason='late_answer')""", (t_ab,)).fetchone()[0]
    talk = c.execute("""SELECT AVG(ended_at - connected_at) FROM calls
                        WHERE state='COMPLETED' AND connected_at IS NOT NULL AND ended_at > ?""",
                     (now - LONG_S,)).fetchone()[0]
    tta = [r[0] for r in c.execute("""SELECT answered_at - created_at FROM calls
                                      WHERE answered_at > ? ORDER BY 1""", (now - LONG_S,))]
    return Snapshot(
        now=now, agent_counts=counts, available=counts.get("AVAILABLE", 0),
        wrap_until=wraps, connected_elapsed=conn_el,
        unbound_inflight=inflight[0] or 0, unbound_ages=ages, bound_inflight=inflight[1] or 0,
        answered_waiting=waiting,
        short=_window(c, now, SHORT_S), long=_window(c, now, LONG_S),
        abandon_answered=ab_ans, abandon_count=ab_cnt,
        avg_talk_s=talk,
        avg_time_to_answer_s=(sum(tta) / len(tta)) if tta else None,
        min_time_to_answer_s=tta[max(0, int(len(tta) * 0.05) - 1)] if len(tta) >= 20 else None,
        tta_samples=tta,
    )
