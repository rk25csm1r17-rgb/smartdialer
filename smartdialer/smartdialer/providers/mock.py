"""Mock telecom providers running on the simulated clock.

The mock owns a small model of borrower behaviour (answer probability, ring time,
talk time, patience when nobody is on the line) and a provider *profile* that
decides how badly the provider behaves (latency, timeouts that may or may not
have created the call, duplicate / dropped / reordered webhooks, outages).
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Optional

from .base import EventSink, ProviderError, ProviderEvent, ProviderTimeout, TelecomProvider


@dataclass
class ProviderProfile:
    name: str
    setup_delay: tuple = (0.5, 1.5)      # dial accepted -> RINGING
    event_delay: tuple = (0.05, 0.2)     # webhook delivery delay; wide range => reordering
    dial_timeout_prob: float = 0.0       # dial() raises ProviderTimeout
    timeout_created_prob: float = 0.5    # ...but the call was created anyway
    dial_error_prob: float = 0.0         # dial() raises ProviderError (call not created)
    duplicate_prob: float = 0.0          # webhook delivered more than once
    drop_event_prob: float = 0.0         # webhook never delivered


PROFILE_A = ProviderProfile(name="provider_a")
PROFILE_B = ProviderProfile(
    name="provider_b",
    setup_delay=(2.0, 6.0),
    event_delay=(0.1, 4.0),
    dial_timeout_prob=0.05,
    dial_error_prob=0.02,
    duplicate_prob=0.25,
    drop_event_prob=0.01,
)


@dataclass
class Population:
    """Borrower behaviour. `answer_rate` and `talk_time_s` may be functions of time."""
    answer_rate: Callable[[float], float] | float = 0.3
    talk_time_s: Callable[[float], float] | float = 120.0
    ring_time_s: tuple = (3.0, 18.0)     # time from RINGING to pick-up, if they pick up
    no_answer_after_s: float = 30.0
    busy_prob: float = 0.05
    patience_s: float = 6.0              # answered, hears silence, hangs up after this

    def p_answer(self, t: float) -> float:
        return self.answer_rate(t) if callable(self.answer_rate) else self.answer_rate

    def talk(self, t: float, rng: random.Random) -> float:
        mean = self.talk_time_s(t) if callable(self.talk_time_s) else self.talk_time_s
        return max(5.0, rng.expovariate(1.0 / mean))


@dataclass
class _MockCall:
    call_id: str
    pcid: str
    status: str = "dialing"        # dialing | ringing | answered | bridged | ended
    answered: bool = False
    agent_id: Optional[str] = None
    seq: int = 0


class MockProvider(TelecomProvider):
    def __init__(self, profile: ProviderProfile, scheduler, population: Population, seed: int = 0):
        self.name = profile.name
        self.profile = profile
        self.sched = scheduler
        self.pop = population
        self.rng = random.Random(seed)
        self.calls: dict[str, _MockCall] = {}
        self.sink: Optional[EventSink] = None
        self.outage = False
        self._held: list[ProviderEvent] = []     # webhooks the provider couldn't deliver during an outage
        self._n = 0
        self.stats = {"dials": 0, "duplicate_dial_requests": 0, "timeouts": 0, "errors": 0,
                      "events_sent": 0, "events_dropped": 0, "events_duplicated": 0}

    def set_outage(self, on: bool) -> None:
        """During an outage the API times out and webhooks queue up at the provider.
        When it recovers, the backlog is redelivered late and in random order."""
        self.outage = on
        if not on:
            held, self._held = self._held, []
            now = self.sched.clock.now()
            for ev in held:
                self._deliver(ev, now + self.rng.uniform(0.5, 10.0))

    # ---- interface -------------------------------------------------------
    def set_event_sink(self, sink: EventSink) -> None:
        self.sink = sink

    def dial(self, call_id: str, phone: str) -> str:
        if call_id in self.calls:                      # idempotency key hit
            self.stats["duplicate_dial_requests"] += 1
            return self.calls[call_id].pcid
        p = self.profile
        if self.outage:
            self.stats["timeouts"] += 1
            if self.rng.random() < p.timeout_created_prob:
                self._create(call_id)
            raise ProviderTimeout(f"{self.name}: dial timed out (outage)")
        r = self.rng.random()
        if r < p.dial_error_prob:
            self.stats["errors"] += 1
            raise ProviderError(f"{self.name}: dial rejected")
        if r < p.dial_error_prob + p.dial_timeout_prob:
            self.stats["timeouts"] += 1
            if self.rng.random() < p.timeout_created_prob:
                self._create(call_id)
            raise ProviderTimeout(f"{self.name}: dial timed out")
        return self._create(call_id).pcid

    def bridge(self, call_id: str, agent_id: str) -> None:
        if self.outage:
            raise ProviderTimeout(f"{self.name}: bridge timed out")
        c = self.calls.get(call_id)
        if c is None or c.status == "ended":
            raise ProviderError(f"{self.name}: cannot bridge {call_id}, call not live")
        if c.agent_id is not None:
            return                                      # idempotent
        c.agent_id = agent_id
        if c.status == "answered":
            self._start_talk(c)

    def hangup(self, call_id: str) -> None:
        if self.outage:
            raise ProviderTimeout(f"{self.name}: hangup timed out")
        c = self.calls.get(call_id)
        if c is None or c.status == "ended":
            return                                      # idempotent
        c.status = "ended"
        self._emit(c, "COMPLETED", answered=c.answered, reason="hangup")

    def lookup(self, call_id: str) -> Optional[str]:
        if self.outage:
            raise ProviderTimeout(f"{self.name}: lookup timed out")
        c = self.calls.get(call_id)
        return None if c is None else c.status

    # ---- behaviour -------------------------------------------------------
    def _create(self, call_id: str) -> _MockCall:
        self._n += 1
        self.stats["dials"] += 1
        c = _MockCall(call_id=call_id, pcid=f"{self.name}-{self._n}")
        self.calls[call_id] = c
        now = self.sched.clock.now()
        setup = self.rng.uniform(*self.profile.setup_delay)
        pop = self.pop

        def ringing():
            if c.status != "dialing":
                return
            c.status = "ringing"
            self._emit(c, "RINGING")
            if self.rng.random() < pop.busy_prob:
                self.sched.after(0.5, lambda: self._fail(c, "busy"))
            elif self.rng.random() < pop.p_answer(self.sched.clock.now()):
                self.sched.after(self.rng.uniform(*pop.ring_time_s), lambda: self._answer(c))
            else:
                self.sched.after(pop.no_answer_after_s, lambda: self._fail(c, "no_answer"))

        self.sched.at(now + setup, ringing)
        return c

    def _fail(self, c: _MockCall, reason: str):
        if c.status in ("ended", "answered", "bridged"):
            return
        c.status = "ended"
        self._emit(c, "FAILED", reason=reason)

    def _answer(self, c: _MockCall):
        if c.status != "ringing":
            return
        c.status = "answered"
        c.answered = True
        self._emit(c, "ANSWERED")
        if c.agent_id is not None:
            self._start_talk(c)

        def impatient():  # nobody bridged: borrower hears silence and hangs up
            if c.status == "answered":
                c.status = "ended"
                self._emit(c, "COMPLETED", answered=True, reason="borrower_hangup_silence")

        self.sched.after(self.pop.patience_s, impatient)

    def _start_talk(self, c: _MockCall):
        c.status = "bridged"
        talk = self.pop.talk(self.sched.clock.now(), self.rng)

        def end():
            if c.status == "bridged":
                c.status = "ended"
                self._emit(c, "COMPLETED", answered=True, reason="normal")

        self.sched.after(talk, end)

    def _emit(self, c: _MockCall, etype: str, answered: bool = False, reason: str = ""):
        c.seq += 1
        now = self.sched.clock.now()
        ev = ProviderEvent(provider=self.name, event_id=f"{c.pcid}:{etype}", call_id=c.call_id,
                           provider_call_id=c.pcid, type=etype, at=now, answered=answered, reason=reason)
        p = self.profile
        if self.outage:
            self._held.append(ev)
            return
        if self.rng.random() < p.drop_event_prob:
            self.stats["events_dropped"] += 1
            return
        self._deliver(ev, now + self.rng.uniform(*p.event_delay))
        if self.rng.random() < p.duplicate_prob:
            self.stats["events_duplicated"] += 1
            # half the duplicates are webhook retries (same event id), half are
            # semantic duplicates with a fresh id: both must be harmless.
            dup = ev if self.rng.random() < 0.5 else ProviderEvent(**{**ev.__dict__, "event_id": ev.event_id + ":dup"})
            self._deliver(dup, now + self.rng.uniform(*p.event_delay) + self.rng.uniform(0, 3))

    def _deliver(self, ev: ProviderEvent, at: float):
        self.stats["events_sent"] += 1
        self.sched.at(at, lambda: self.sink and self.sink(ev))
