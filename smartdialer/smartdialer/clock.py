"""Injectable clocks so the same code runs in real time and in the discrete-event simulator."""
from __future__ import annotations

import time


class RealClock:
    def now(self) -> float:
        return time.time()


class SimClock:
    def __init__(self, start: float = 0.0):
        self._t = start

    def now(self) -> float:
        return self._t

    def advance_to(self, t: float) -> None:
        if t < self._t:
            raise ValueError("time cannot go backwards")
        self._t = t
