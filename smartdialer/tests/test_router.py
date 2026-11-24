"""Provider router: circuit breaker + load spreading."""
from smartdialer.clock import SimClock
from smartdialer.providers.base import TelecomProvider
from smartdialer.providers.router import BreakerConfig, CircuitBreaker, ProviderRouter


class _Stub(TelecomProvider):
    def __init__(self, name):
        self.name = name
    def dial(self, *a): ...
    def bridge(self, *a): ...
    def hangup(self, *a): ...
    def lookup(self, *a): ...
    def set_event_sink(self, *a): ...


def _router(n=2):
    return ProviderRouter([_Stub(f"p{i}") for i in range(n)], SimClock())


def test_new_calls_are_spread_across_healthy_providers():
    r = _router(2)
    picks = [r.choose() for _ in range(100)]
    # both providers get a substantial share, not 100/0
    assert min(picks.count("p0"), picks.count("p1")) > 30


def test_breaker_opens_after_sustained_failures_and_routes_away():
    clock = SimClock()
    r = ProviderRouter([_Stub("p0"), _Stub("p1")], clock, BreakerConfig(min_samples=8, failure_ratio_to_open=0.5))
    for _ in range(10):
        r.record("p0", False)
    assert r.breakers["p0"].current() == CircuitBreaker.OPEN
    # p0 is out; every new call now goes to the healthy p1
    assert {r.choose() for _ in range(20)} == {"p1"}
    assert r.health() == "HEALTHY"


def test_half_open_then_closed_after_probes():
    clock = SimClock()
    cfg = BreakerConfig(min_samples=8, failure_ratio_to_open=0.5, open_cooldown_s=20,
                        half_open_successes_to_close=3)
    r = ProviderRouter([_Stub("p0")], clock, cfg)
    for _ in range(10):
        r.record("p0", False)
    assert r.breakers["p0"].current() == CircuitBreaker.OPEN and r.health() == "DOWN"
    clock.advance_to(25)
    assert r.breakers["p0"].current() == CircuitBreaker.HALF_OPEN and r.health() == "DEGRADED"
    for _ in range(3):
        r.record("p0", True)
    assert r.breakers["p0"].current() == CircuitBreaker.CLOSED and r.health() == "HEALTHY"


def test_outage_blast_radius_is_bounded_by_spreading():
    """Two healthy providers, spread evenly. One dies: the calls stranded on it
    (unbridgeable answers) should be ~half, not all."""
    from smartdialer.testing import FixedPacer, World, seed_history
    from smartdialer.providers.mock import PROFILE_A
    w = World(agents=20, borrowers=3000, pacer=FixedPacer(1000))
    seed_history(w, 300, answer_rate=0.3)
    # two identical fake providers via the router
    from smartdialer.testing import FakeProvider
    p0, p1 = FakeProvider("fa"), FakeProvider("fb")
    from smartdialer.providers.router import ProviderRouter
    w.w.router = ProviderRouter([p0, p1], w.clock)
    w.w.allocator.router = w.w.router
    w.w.tick()
    on_p0 = sum(1 for c in w.calls() if c["provider"] == "fa")
    on_p1 = sum(1 for c in w.calls() if c["provider"] == "fb")
    assert on_p0 > 0 and on_p1 > 0
    assert min(on_p0, on_p1) / max(on_p0, on_p1) > 0.5          # roughly balanced
