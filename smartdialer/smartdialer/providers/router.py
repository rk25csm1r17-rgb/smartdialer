"""Provider selection + per-provider circuit breaker.

Important rule: a call is pinned to the provider it was first sent to. A dial
that timed out at provider A may still be ringing the borrower, so retrying the
*same call* on provider B could ring them twice. Only *new* calls fail over.

Breaker state is per worker (in memory). Workers converge independently within
one window; the Safety Controller also looks at DB-level outcomes, so a single
worker with a stale breaker cannot cause unsafe dialing. See docs/ADR.md.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .base import TelecomProvider


@dataclass(frozen=True)
class BreakerConfig:
    window: int = 20
    min_samples: int = 8
    failure_ratio_to_open: float = 0.5
    open_cooldown_s: float = 20.0
    half_open_successes_to_close: int = 3


class CircuitBreaker:
    CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

    def __init__(self, clock, cfg: BreakerConfig = BreakerConfig()):
        self.clock, self.cfg = clock, cfg
        self.state = self.CLOSED
        self.results: deque = deque(maxlen=cfg.window)
        self.opened_at = 0.0
        self.half_open_ok = 0
        self.transitions: list = []

    def _set(self, s):
        if s != self.state:
            self.transitions.append((round(self.clock.now(), 1), self.state, s))
            self.state = s

    def current(self) -> str:
        if self.state == self.OPEN and self.clock.now() - self.opened_at >= self.cfg.open_cooldown_s:
            self._set(self.HALF_OPEN)
            self.half_open_ok = 0
        return self.state

    def record(self, ok: bool) -> None:
        st = self.current()
        if st == self.HALF_OPEN:
            if ok:
                self.half_open_ok += 1
                if self.half_open_ok >= self.cfg.half_open_successes_to_close:
                    self.results.clear()
                    self._set(self.CLOSED)
            else:
                self.opened_at = self.clock.now()
                self._set(self.OPEN)
            return
        self.results.append(ok)
        fails = self.results.count(False)
        if (len(self.results) >= self.cfg.min_samples
                and fails / len(self.results) >= self.cfg.failure_ratio_to_open):
            self.opened_at = self.clock.now()
            self._set(self.OPEN)


class ProviderRouter:
    def __init__(self, providers: list[TelecomProvider], clock, cfg: BreakerConfig = BreakerConfig()):
        self.providers = {p.name: p for p in providers}
        self.order = [p.name for p in providers]           # preference order
        self.breakers = {p.name: CircuitBreaker(clock, cfg) for p in providers}
        self._choices = 0
        self.probe_every = 10

    def get(self, name: str) -> TelecomProvider:
        return self.providers[name]

    def choose(self) -> str | None:
        """Pick a provider for a NEW call.

        New calls are SPREAD round-robin across every CLOSED provider, not always sent
        to the first one. That bounds the blast radius of a single provider failing: if
        one of two healthy providers dies suddenly, only ~half the in-flight unbound
        calls are riding on it, so at most ~half can answer into a provider that can't
        bridge them. (The old 'first CLOSED' rule put every egg in one basket.)

        A preferred provider that is only HALF_OPEN still gets every `probe_every`-th
        call so it can prove itself and close again; if nothing is CLOSED we fall through
        to any HALF_OPEN provider to keep a trickle flowing while recovering."""
        self._choices += 1
        if self._choices % self.probe_every == 0:
            for n in self.order:
                if self.breakers[n].current() == CircuitBreaker.HALF_OPEN:
                    return n
        closed = [n for n in self.order if self.breakers[n].current() == CircuitBreaker.CLOSED]
        if closed:
            return closed[self._choices % len(closed)]
        for n in self.order:
            if self.breakers[n].current() == CircuitBreaker.HALF_OPEN:
                return n
        return None

    def record(self, name: str, ok: bool) -> None:
        self.breakers[name].record(ok)

    def health(self) -> str:
        """HEALTHY: some provider CLOSED. DEGRADED: only HALF_OPEN ones. DOWN: all OPEN."""
        states = [b.current() for b in self.breakers.values()]
        if CircuitBreaker.CLOSED in states:
            return "HEALTHY"
        if CircuitBreaker.HALF_OPEN in states:
            return "DEGRADED"
        return "DOWN"
