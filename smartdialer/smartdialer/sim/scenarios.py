"""Named scenarios from the assignment, plus disruption scenarios."""
from __future__ import annotations

from ..safety import SafetyConfig
from .runner import Scenario


def changing_answer_rate(t: float) -> float:
    # 0-10 min: 30%, 10-20 min: jumps to 70%, 20-30 min: collapses to 10%
    return 0.3 if t < 600 else (0.7 if t < 1200 else 0.1)


def changing_talk_time(t: float) -> float:
    return 90.0 if t < 600 else (180.0 if t < 1200 else 60.0)


BASE = dict(agents=100, duration_s=1800, workers=3, providers=("A", "B"))

SCENARIOS = {
    "A": dict(answer_rate=0.2, talk_time_s=120, description="20% answer, 120s talk"),
    "B": dict(answer_rate=0.5, talk_time_s=90, description="50% answer, 90s talk"),
    "C": dict(answer_rate=0.7, talk_time_s=180, description="70% answer, 180s talk"),
    "D": dict(answer_rate=changing_answer_rate, talk_time_s=changing_talk_time,
              description="answer 30% -> 70% (t=10m) -> 10% (t=20m); talk 90 -> 180 -> 60s"),
    # disruption scenarios
    "outage": dict(answer_rate=0.3, talk_time_s=90, providers=("A",), duration_s=1200,
                   script=[(600, "outage", "provider_a", 120)],
                   description="single provider, 2-minute outage at t=10m"),
    "outage_failover": dict(answer_rate=0.3, talk_time_s=90, providers=("A", "B"), duration_s=1200,
                            script=[(600, "outage", "provider_a", 120)],
                            description="provider A outage at t=10m, B available for new calls"),
    "agent_drop": dict(answer_rate=0.3, talk_time_s=90, duration_s=1200,
                       script=[(600, "drop_agents", 40, 3.0)],
                       description="100 agents, 40 log out within 3s at t=10m"),
    "worker_crash": dict(answer_rate=0.3, talk_time_s=90, duration_s=900, agents=50,
                         script=[(300, "crash_worker", 0, 60), (305, "crash_worker", 1, None)],
                         description="w0 crashes at 5m (restarts at 6m), w1 dies for good at 5m05s"),
}


def build(name: str, mode: str = "predictive", **overrides) -> Scenario:
    cfg = {**BASE, **SCENARIOS[name], **overrides}
    return Scenario(name=name, mode=mode, **cfg)
