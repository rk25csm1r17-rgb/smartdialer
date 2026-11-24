# Simulation results

Reproduce everything here with:

```bash
python -m smartdialer.sim A B C D                       # the comparison table below
python -m smartdialer.sim D --mode predictive --timeline   # the per-minute D walkthrough
python -m smartdialer.sim outage outage_failover agent_drop worker_crash --timeline
python -m smartdialer.explain B --at 600                # one decision, fully itemised
python scripts/loadtest.py                              # throughput + the safety-math bottleneck
python -m pytest -q tests/test_chaos.py                 # 240 adversarial fuzz runs
```

All runs: 100 agents, 3 worker processes, 30 minutes of simulated time, providers A **and** B (B has timeouts, duplicate, dropped and reordered webhooks). Utilization is measured from agent-state time integration, not self-reported. **Invariant violations across every run: 0.** The checker (`invariants.py`) runs every 10 simulated seconds and asserts, among other things, that agent-bound active calls never exceed the agent count and no agent or borrower ever has two active calls.

## Scenarios A–D

| Scenario | Mode | Talk utilization | Occupancy (incl. wrap) | Calls initiated | Answered | Connected | Abandoned | Abandon % | Invariant errors |
|---|---|---|---|---|---|---|---|---|---|
| **A** 20% / 120 s | progressive | 42.9% | 46.5% | 3,710 | 700 | 700 | 0 | 0.00% | 0 |
| | **predictive** | **68.5%** | **74.5%** | 6,104 | 1,142 | 1,142 | 0 | 0.00% | 0 |
| **B** 50% / 90 s | progressive | 62.9% | 69.6% | 2,682 | 1,260 | 1,260 | 0 | 0.00% | 0 |
| | **predictive** | **80.3%** | **88.8%** | 3,384 | 1,575 | 1,575 | 0 | 0.00% | 0 |
| **C** 70% / 180 s | progressive | 83.5% | 88.0% | 1,317 | 895 | 895 | 0 | 0.00% | 0 |
| | **predictive** | **88.1%** | **93.2%** | 1,534 | 985 | 985 | 0 | 0.00% | 0 |
| **D** changing | progressive | 54.9% | 59.3% | 3,031 | 808 | 808 | 0 | 0.00% | 0 |
| | **predictive** | **68.2%** | **74.2%** | 5,432 | 1,113 | 1,113 | 0 | 0.00% | 0 |

Reading it:
* The lower the answer rate, the more predictive helps: **+26 points** of utilization at 20% answer (scenario A), **+5** at 70% (scenario C). At 70% there just isn't much idle time for predictive to reclaim, and the safety bound keeps it from chasing the last few points into abandons.
* Predictive connects **1.6× / 1.25× / 1.10× / 1.38×** as many borrowers as progressive in the same half hour, for the same 100 agents.
* Abandon rate is **0.00%** everywhere here (the safety bound is conservative by design). Provider B's misbehaviour is fully absorbed — see the `event_effects` and `provider_stats` blocks in `results/*.json` for the duplicate/dropped/reordered counts that were handled.

## Scenario D: the answer rate changes under the dialer

D holds answer rate at 30% for 10 min, jumps it to **70%** at t=10 min, then collapses it to **10%** at t=20 min (talk time changes too). Predictive, per minute (`util_now` = fraction of logged-in agents talking right now; `fallback` = decisions forced to progressive that minute, out of 180):

```
t_min  util  initiated  answered  connected  abandoned  fallback/180
 10    0.79     137        46        46          0          0      steady at 30%
 11    0.92     122        56        56          0          0      ← answer rate just jumped to 70%
 12    0.80      72        26        26          0         78      drift rule fires: answers rising
 13    0.84      56        35        35          0        180      full fallback to progressive
 ...   (safe: utilization stays high on the back of the surprise answers, nobody abandoned)
 20    0.78      58        30        31          0        167      ← answer rate just collapsed to 10%
 21    0.63     156        12        12          0         34      short window catches up, predictive resumes
 22    0.57     259        14        14          0          0      dialing hard into a 10% population
 ...
 30    0.42     369        34        34          0          0      ~370 dials/min to keep agents busy at 10%
```

The two transitions are handled in opposite ways, on purpose:
* **30% → 70% (dangerous):** more borrowers answer than expected. The drift detector (short-window answer rate significantly above long-window, z > 3) forces **progressive** within ~1 minute and keeps it there while the windows disagree. Zero abandons through the spike.
* **70% → 10% (safe):** agents go idle. The pacer's short window drops within ~2 minutes and it ramps dialing up to ~370 calls/minute. The controller's `max(UCB)` keeps it cautious until the long window catches up, so the ramp is gradual, not a lurch.

Abandoned calls through all of D: **0**.

## Disruptions

30 min, 100 agents unless noted. Full timelines: `python -m smartdialer.sim <name> --timeline`.

| Scenario | What it shows | Headline |
|---|---|---|
| `outage` | Single provider goes dark at t=10 min for 2 min | Breaker opens ~11 s after failures start → new calls `REJECT`; in-flight calls time out to FAILED and hang up; borrowers get backoff, **0 attempts charged**; recovers to HEALTHY by itself; then stays in fallback for a while because the outage's abandons used up the abandon budget. |
| `outage_failover` | Same, but provider B is available | New calls are spread across A **and** B all along, so when A dies B is already carrying half the load: utilization holds at **~71%** with a **1.4%** abandon rate through the outage (vs ~61% / 3.6% when everything was routed to one provider first). A is probed back half-open → closed. |
| `agent_drop` | 40 of 100 agents log out within 3 s at t=10 min | Reaction within one tick (≤1 s): approvals → 0 and the newest unanswered calls are shed until `P(answers > capacity)` is back under 5%. |
| `worker_crash` | w0 dies at 5 min (restarts at 6), w1 dies for good at 5 min 05 s | No stuck calls, no lost agents; surviving workers pick up leases and timers; utilization barely dips. |

The per-process economics are in `python scripts/loadtest.py` and discussed in `docs/ARCHITECTURE.md` §9.

## Correctness under chaos (`tests/test_chaos.py`)

240 randomized runs (120 progressive, 120 predictive), each a different hostile interleaving: provider events reordered / duplicated / dropped, workers crashing before and after the event commit, agents churning, multiple workers on one database. Invariants are asserted after **every** step and the run then drains to a fully consistent terminal state.

* **0 invariant violations across all 240 runs.**
* The fuzzer found one real bug on its first outing — call-id generation used a per-worker seeded PRNG, so a restarted worker could regenerate ids and collide, aborting the allocation transaction. Fixed with truly-random ids. Every failure prints its `seed`/`mode` so it replays deterministically.

Total suite: **293 tests** (was 49), ~16 s.
