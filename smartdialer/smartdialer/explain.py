"""Answer 'why did the system decide to make this many calls right now?'

    python -m smartdialer.explain [scenario] [--at SECONDS]

Runs the scenario (predictive) up to the given time and prints the last
pacing proposal with all its inputs, then the Safety Controller's decision
with its own inputs and reasons.
"""
from __future__ import annotations

import argparse
import json

from .sim.runner import Simulation
from .sim.scenarios import build


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m smartdialer.explain")
    ap.add_argument("scenario", nargs="?", default="B")
    ap.add_argument("--at", type=float, default=600)
    a = ap.parse_args(argv)
    sim = Simulation(build(a.scenario, "predictive", duration_s=a.at))
    sim.run()
    c = sim.store.read()
    rows = c.execute("""SELECT ts, worker, mode, requested, approved, action, detail FROM safety_decisions
                        ORDER BY ts DESC LIMIT 1""").fetchall()
    for ts, worker, mode, req, ap_, action, detail in rows:
        d = json.loads(detail)
        p = d.pop("pacer")
        print(f"t={ts:.1f}s worker={worker}\n")
        print("PACING ENGINE (predictive) proposed:", p.get("n"))
        print(f"  answer rate p           = {p['p_answer']}  (from {p['p_source']})")
        print(f"  horizon H (dial->answer) = {p['horizon_s']}s, avg talk = {p['avg_talk_s']}s")
        print(f"  free now                 = {p['free_now']}")
        print(f"  + wrap-ups ending in H   = {p['wrap_free']}")
        print(f"  + talks ending in H      = {p['talk_free']}")
        print(f"  = expected free agents   = {p['expected_free']}")
        print(f"  - expected answers from {p['unbound_inflight']} calls already ringing = {p['inflight_expected_answers']}")
        print(f"  = need                   = {p['need']}  ->  need / p = {p['raw']}")
        print(f"  x aggressiveness {p['aggressiveness']} (abandon rate {p['abandon_rate']}), cap {p['cap']}  ->  n = {p['n']}")
        print(f"\nSAFETY CONTROLLER: {action}  requested={req} approved={ap_} mode={mode}")
        print(f"  its own capacity         = available {d['available']} - answered waiting {d['answered_waiting']}"
              f" + wrap-ups certainly done before earliest answer {d['wrap_credit']} = {d['capacity']}")
        print(f"  answer rate it trusts    = max(UCB short {d['ucb_short']}, UCB long {d['ucb_long']}) = {d['p_safe']}")
        print(f"  overflow budget eps      = {d['eps']}  (abandon rate {d['abandon_rate']})")
        if "overflow_p_after" in d:
            print(f"  P(answers > capacity)    = {d['inflight_overflow_p']} now -> {d['overflow_p_after']} after +{d['allowed_new']} calls")
        print(f"  reasons                  = {d['reasons'] or ['within limits']}")
        print(f"  fallback triggers        = {d['fallback'] or 'none'}")


if __name__ == "__main__":
    main()
