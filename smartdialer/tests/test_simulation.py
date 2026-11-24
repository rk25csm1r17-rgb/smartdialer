"""End-to-end: short simulations with the misbehaving provider B in the mix,
3 workers, invariants checked every 10 simulated seconds."""
import pytest

from smartdialer.sim.runner import Simulation
from smartdialer.sim.scenarios import build


@pytest.mark.parametrize("name", ["A", "B", "C"])
def test_predictive_beats_progressive_safely(name):
    kw = dict(agents=30, duration_s=480, borrowers=6000)
    prog = Simulation(build(name, "progressive", **kw)).run()
    pred = Simulation(build(name, "predictive", **kw)).run()
    for r in (prog, pred):
        assert r["invariant_violations"] == 0, r["invariant_examples"]
    assert pred["talk_utilization"] >= prog["talk_utilization"]
    assert pred["abandon_rate"] <= 0.03


def test_provider_b_misbehaviour_is_absorbed():
    r = Simulation(build("B", "predictive", agents=20, duration_s=420, borrowers=4000, providers=("B",))).run()
    assert r["invariant_violations"] == 0, r["invariant_examples"]
    assert r["event_effects"].get("duplicate_delivery", 0) > 0          # B really did send duplicates
    assert r["provider_stats"]["provider_b"]["timeouts"] > 0
