"""CLI:  python -m smartdialer.sim [scenario ...] [--mode predictive|progressive|both] [--minutes N]
             [--agents N] [--epsilon X] [--timeline] [--json out.json]"""
from __future__ import annotations

import argparse
import json
import time

from ..safety import SafetyConfig
from .report import print_comparison, print_timeline
from .runner import Simulation
from .scenarios import SCENARIOS, build


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m smartdialer.sim")
    ap.add_argument("scenarios", nargs="*", default=["A", "B", "C", "D"], help=f"any of {list(SCENARIOS)}")
    ap.add_argument("--mode", default="both", choices=["predictive", "progressive", "both"])
    ap.add_argument("--minutes", type=float)
    ap.add_argument("--agents", type=int)
    ap.add_argument("--epsilon", type=float, help="Safety Controller per-decision overflow risk")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--timeline", action="store_true", help="print per-minute timeline")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--json", help="write all summaries + timelines to this file")
    a = ap.parse_args(argv)
    modes = ["progressive", "predictive"] if a.mode == "both" else [a.mode]
    results = []
    for name in a.scenarios:
        for mode in modes:
            ov = {"seed": a.seed}
            if a.minutes:
                ov["duration_s"] = a.minutes * 60
            if a.agents:
                ov["agents"] = a.agents
            if a.epsilon is not None:
                ov["safety"] = SafetyConfig(risk_epsilon=a.epsilon)
            sc = build(name, mode, **ov)
            t = time.time()
            sim = Simulation(sc, verbose=a.verbose)
            summary = sim.run()
            summary["wall_s"] = round(time.time() - t, 1)
            summary["description"] = sc.description
            results.append({"summary": summary, "timeline": sim.timeline, "log": sim.log})
            if a.timeline:
                print_timeline(summary, sim.timeline, sim.log)
    print_comparison([r["summary"] for r in results])
    if a.json:
        with open(a.json, "w") as f:
            json.dump(results, f, indent=1, default=str)
        print(f"\nwrote {a.json}")


if __name__ == "__main__":
    main()
