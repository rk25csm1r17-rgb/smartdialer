"""Minimal discrete-event scheduler driving a SimClock."""
from __future__ import annotations

import heapq
import itertools
from typing import Callable

from ..clock import SimClock


class Scheduler:
    def __init__(self, clock: SimClock):
        self.clock = clock
        self._q: list = []
        self._seq = itertools.count()

    def at(self, t: float, fn: Callable[[], None]) -> None:
        heapq.heappush(self._q, (max(t, self.clock.now()), next(self._seq), fn))

    def after(self, dt: float, fn: Callable[[], None]) -> None:
        self.at(self.clock.now() + dt, fn)

    def every(self, period: float, fn: Callable[[], None], start: float | None = None, until: float | None = None) -> None:
        def tick():
            fn()
            nxt = self.clock.now() + period
            if until is None or nxt <= until:
                self.at(nxt, tick)
        self.at(self.clock.now() if start is None else start, tick)

    def run_until(self, t_end: float) -> None:
        while self._q and self._q[0][0] <= t_end:
            t, _, fn = heapq.heappop(self._q)
            self.clock.advance_to(t)
            fn()
        self.clock.advance_to(max(self.clock.now(), t_end))

    def run_all(self, limit: float = 1e9) -> None:
        self.run_until(limit)
