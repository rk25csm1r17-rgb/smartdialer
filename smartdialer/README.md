# SmartDialer

A working prototype of an outbound collections dialer with **progressive** and **predictive** pacing, a non-bypassable **Safety Controller** between the pacing logic and the telecom provider, and correct agent/call allocation across multiple workers.

> Assignment answers live in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) (diagrams, state machines, concurrency, scale), [`docs/ADR.md`](docs/ADR.md) (what/why/solves/harder for every choice), and [`docs/RESULTS.md`](docs/RESULTS.md) (scenario numbers). The final question is answered at the bottom of this file.

## TL;DR of the design

```
Campaign ─▶ Pacing Engine ─"n calls"─▶ │ Safety Controller ─▶ Call Allocator │ ─▶ Provider router ─▶ Telecom A/B
           (predictive/progressive)     └──── one DB transaction ────────────┘
```

* **One SQLite database (WAL) is the single source of truth.** Every state change is a compare-and-set (`UPDATE ... WHERE id=? AND state=?`); the decide-and-claim step is one serialised transaction; partial unique indexes make a double-booked agent or borrower impossible to commit. No Redis/Kafka/microservices — see [ADR-2](docs/ADR.md) for why, and §9 of the architecture doc for where that stops scaling and what replaces it.
* **The pacer only proposes a number.** It imports nothing that can write or dial (enforced by a test). The Safety Controller, inside the allocator's transaction, reads its *own* snapshot and decides how many calls — if any — are actually safe, bounding the expected abandon rate with a Poisson-binomial model. It cannot be switched off from the pacing path (`__slots__`, frozen config, read-only).
* **Crashes, duplicate/out-of-order/lost provider events, provider outages and sudden agent loss are handled** by a transactional outbox, an event inbox, forward-only call states, and a reconciler of timers — all idempotent and runnable by any worker.

## Requirements

Python **3.11+**, standard library only. `pytest` for the tests.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pytest        # the only dependency, and only for tests
```

No database to install, no services to start.

## Run it

```bash
# Scenarios A–D from the brief (progressive vs predictive, 30 min, providers A+B):
python -m smartdialer.sim A B C D

# One scenario with the per-minute timeline:
python -m smartdialer.sim D --mode predictive --timeline

# Disruptions: provider outage, failover, mass agent logout, worker crashes:
python -m smartdialer.sim outage outage_failover agent_drop worker_crash --timeline

# "Why this many calls right now?" — one decision, every input shown:
python -m smartdialer.explain B --at 600

# The five failure cases, narrated step by step:
python -m smartdialer.demos                 # or: python -m smartdialer.demos crash

# Tests (state machines, concurrency across threads AND processes, events, safety, failures, e2e):
python -m pytest -q

# The fuzzer on its own: 240 adversarial runs that try hard to break correctness:
python -m pytest -q tests/test_chaos.py

# Basic load test: full write path under 1/2/4/8 processes + the safety-math cost at scale:
python scripts/loadtest.py
```

Handy knobs: `--minutes N`, `--agents N`, `--epsilon X` (Safety Controller per-decision overflow risk), `--seed N`, `--json out.json`.

## Layout

```
smartdialer/
  states.py        agent + call state machines (the forward-only rank rule lives here)
  store.py         SQLite schema, BEGIN IMMEDIATE transactions, CAS helpers, unique indexes
  calls.py         core transitions: claim agent, connect answered, finalize, rebind/cancel
  snapshot.py      point-in-time campaign view built only from the DB (+ age-adjusted answer prob)
  pacing.py        ProgressivePacer, PredictivePacer  — PROPOSE a number, nothing else
  safety.py        SafetyController + Poisson-binomial OverflowRisk — the only thing that authorises calls
  allocator.py     CallAllocator (decide-and-claim tx) + outbox dispatcher (DIAL/BRIDGE/HANGUP)
  events.py        provider webhook ingestion: inbox dedupe + forward-only application
  agents.py        login/ready/pause/logout/heartbeat, incl. agent-vanishes-mid-setup
  reconciler.py    the timers: wrap-up, abandon grace, setup/ring timeouts, lost heartbeats
  worker.py        one worker = reconcile → snapshot → pace → allocate → dispatch
  invariants.py    global invariants, checked every 10 s in the sim and in tests
  providers/       base interface, router+circuit-breaker, two mock providers (A fast, B nasty)
  sim/             discrete-event simulator, scenarios, CLI, report
  providers/router.py  circuit breakers + spreads new calls across healthy providers
  demos.py         the five failure-case walkthroughs
  explain.py       itemised pacing + safety decision
tests/             293 tests, incl. test_chaos.py (randomized fuzzer) and test_router.py
scripts/loadtest.py
docs/              ARCHITECTURE.md · ADR.md · RESULTS.md
```

## Headline results (30 min, 100 agents, providers A+B, measured — details in docs/RESULTS.md)

| Scenario | Talk utilization (progressive → predictive) | Borrowers connected | Abandon % | Invariant violations |
|---|---|---|---|---|
| A — 20% answer / 120 s | 42.9% → **68.5%** | 700 → **1,142** | 0.00% | 0 |
| B — 50% answer / 90 s | 62.9% → **80.3%** | 1,260 → **1,575** | 0.00% | 0 |
| C — 70% answer / 180 s | 83.5% → **88.1%** | 895 → **985** | 0.00% | 0 |
| D — changing | 54.9% → **68.2%** | 808 → **1,113** | 0.00% | 0 |

## A note on the mock providers

`providers/mock.py` ships provider **A** (fast, reliable) and provider **B** (slow setup, 5% dial timeouts of which half still create the call, 2% hard errors, 25% duplicate webhooks — half retries, half fresh ids — plus dropped and heavily reordered events). The dialer only ever sees the `TelecomProvider` interface. A real Plivo/Twilio adapter is a drop-in implementation of that interface; I didn't wire a live account (the brief marks it optional), but the seams are there: `dial/bridge/hangup/lookup` + a webhook that calls `EventIngestor.ingest`.

---

## Final question — getting predictive's utilization with progressive's deterministic safety

Short version: **keep the safety boundary deterministic and keep the pacing optimistic, and put the only coupling between them at a single serialised commit.**

1. **Never let the predictor place a call.** Predictive output is a *request*; a separate controller converts requests to authorisations. The authorisation and the resulting calls are written in one serialised transaction that reads capacity itself, so predictive can be as aggressive as it likes and still never commit more calls than the deterministic rule allows. That is the progressive guarantee — "we only ever create as many agent-bound calls as we can staff" — generalised: progressive is just the special case ε = 0, one agent per call.

2. **Make safe capacity bigger without making it a guess.** Most of progressive's idle time is agents in wrap-up and calls about to end. Wrap-up is *system-timed*, so an agent whose wrap-up ends before the earliest plausible answer is capacity you can bank with certainty, not a forecast. Banking only the deterministic part of near-future capacity recovers a large chunk of the utilization gap with zero added abandonment risk. (In scenario C, nearly all of predictive's edge is this.)

3. **Spend the remaining risk explicitly and in small quanta.** For the genuinely probabilistic part (will a ringing borrower answer?), admit calls one at a time while the *exact* overflow probability stays under a tiny per-decision ε, using a conservative upper bound on the answer rate and each call's age-adjusted answer probability. The abandon rate is then a dial you set, not an emergent surprise — and it's checked against the regulatory ceiling continuously, with automatic fallback to pure progressive when data is thin, when the answer rate is rising, or when anyone is already answered and waiting.

4. **Fail toward progressive, fast.** The expensive direction is "more answers than agents". Detect capacity loss (agents vanish) and answer-rate spikes within one worker tick and *shed* already-ringing calls, or drop to ε = 0, before the borrower can be abandoned. A dropping answer rate is the cheap direction — you lose some utilization while the estimate catches up, never safety.

So the knob is a single ε. ε = 0 is exactly progressive; a small ε buys most of predictive's utilization for a bounded, auditable abandonment risk; and every boundary condition collapses the system back to deterministic progressive behaviour automatically. If I had another week: replace the per-decision snapshot with incrementally-maintained counters (the real scaling bottleneck, measured in the load test), shard the serialised commit by agent pool so it scales past ~1,000 agents, tighten the per-provider in-flight exposure further (new calls are already spread across healthy providers so a single outage strands only its share), and wire a live Plivo adapter behind the existing interface.
