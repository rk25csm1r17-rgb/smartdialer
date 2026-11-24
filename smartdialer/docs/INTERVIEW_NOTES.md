# SmartDialer — interview preparation

This is your cheat-sheet for the technical discussion. It is written so you can **defend** the system, not recite it: every answer has the reasoning behind it. Read it with the code open (the file/function map is at the end). If you remember one thing, remember this sentence, because almost every answer reduces to it:

> **The database is the only source of truth; every state change is a conditional `UPDATE ... WHERE state = <what I just read>`; and the predictor can only ever propose a number — the Safety Controller, inside the same transaction that writes the calls, decides what is actually allowed.**

---

## 0. The 30-second pitch

"It's an outbound dialer with progressive and predictive pacing. The hard requirement is that an abandoned connected call is a compliance problem, so there's a Safety Controller between the pacing logic and the telecom provider that the predictor cannot bypass. Everything — agents, calls, the command outbox, the event inbox, every decision — lives in one SQLite database used with compare-and-set updates and partial unique indexes, so multiple stateless workers can run the same campaign without ever double-booking an agent. I chose boring, provable building blocks over impressive ones, and I load-tested and fuzzed it to find where it actually breaks."

---

## 1. The questions the brief literally says they'll ask

### Q. "Two workers try to reserve the same agent at exactly the same time. Walk us through what happens."

Both workers may **read** the agent as `AVAILABLE` — reading isn't reserving. The reservation is one statement:

```sql
UPDATE agents SET state='RESERVED', call_id=:call, version=version+1, state_since=:now
WHERE id = (SELECT id FROM agents WHERE state='AVAILABLE' ORDER BY state_since LIMIT 1)
  AND state = 'AVAILABLE'
RETURNING id;
```

The database applies the two updates one after another. The first flips the agent to `RESERVED` and returns its id. The second re-evaluates `AND state='AVAILABLE'` against the now-committed row, sees `RESERVED`, matches **zero rows**, and that worker gets nothing (or the next agent). The winner is decided by the database's row serialization, not by application timing.

Two backstops: (1) a **partial unique index** `one_active_call_per_agent ON calls(agent_id) WHERE state IN (active states)` makes a second active call for the same agent impossible to *commit* even if the app logic were wrong; (2) in this prototype the whole decide-and-claim block runs under `BEGIN IMMEDIATE`, which takes the write lock up front, so the sequence can't interleave at all.

*Proof it works:* `tests/test_concurrency.py` — `test_cas_without_lock_two_readers_one_winner` shows the CAS alone is enough even without the lock, `test_threads_race_for_one_agent` runs 16 threads, `test_processes_never_overbook_agents` runs **8 OS processes** against 20 agents and audits that nobody was double-booked.

### Q. "Your database says AVAILABLE, your cache says RESERVED. Which one wins?"

The database, always. **There is no cache in the write path** — that's the point of the design. The only operation that can reserve an agent is the conditional UPDATE above, against the DB. If I added a cache later (for a dashboard, or as a pacing hint), it would be allowed to be stale in *both* directions, precisely because it can never be the thing that authorizes a reservation. The classic cache/DB-divergence bug can't exist here because the cache is never authoritative.

### Q. "The provider sends ANSWERED, your worker crashes, then COMPLETED arrives. What happens?"

Two sub-cases, both safe:

- **Crashed before the DB commit:** nothing happened. The `provider_events` row and the state change are in the *same* transaction, and webhooks are only acked after commit, so the provider redelivers ANSWERED. Another worker applies it exactly once. Then COMPLETED is processed normally.
- **Crashed after the commit:** the call and agent are durably `CONNECTED`, and a `BRIDGE` command is sitting in the outbox. The dead worker's command lease (2 s) expires and any worker executes the BRIDGE — idempotently, because our call id is the provider's idempotency key. The redelivered ANSWERED is a `duplicate_delivery` no-op. COMPLETED then closes the call and sends the agent to `WRAP_UP`.

*Proof:* `tests/test_events.py::test_crash_after_answered_commit_then_completed`, and the narrated `python -m smartdialer.demos crash`.

### Q. "Your model predicted a 70% answer rate. It suddenly drops to 10%. How does the system protect itself?"

A dropping answer rate is the **safe** direction — it means agents sit idle, nobody gets abandoned. So "protection" here is really "don't over-correct and don't get fooled by the opposite":
- The pacer's short window notices within ~2 minutes and asks for many more calls to fill the idle agents.
- The Safety Controller keeps using `max(Wilson-upper-bound(short window), Wilson-upper-bound(long window))` for the answer rate it trusts, so until the 15-minute window catches up it still assumes ~70% and only approves what's safe *at 70%*. The ramp-up is gradual, not a lurch. Utilization dips for a while; the abandon bound never moves.

The genuinely dangerous direction is the reverse (10% → 70%: more people answer than you have agents). That trips the **drift rule** — if the short-window rate is significantly above the long-window rate (z > 3), the controller forces pure progressive within the short window. And if anyone is *already answered and waiting for an agent*, it forces progressive immediately.

*Proof:* scenario D does both transitions; `docs/RESULTS.md` has the per-minute table showing fallback kicking in at the 70% jump and dialing ramping up at the 10% collapse, with **0 abandons** throughout.

### Q. "We just went from 1,000 to 100,000 agents. What breaks first?"

In order (and I measured this, I didn't guess — `python scripts/loadtest.py`):

1. **The Safety Controller's exact math** — already hit and fixed. Exact Poisson-binomial is O(in-flight × capacity): ~2 ms at 100 free agents, ~190 ms at 1,000, **~27 s at 10,000** per decision. Above 400 free agents it now uses a normal approximation (O(1) per call, 3 ms at 10,000, matching exact to 3 decimals at the 2% tail).
2. **The snapshot reads, not the writes.** Every decision runs COUNT/AVG queries over the last 15–30 minutes of calls. Fix: maintain those numbers incrementally (counters + histograms updated in the same transactions that change state) so a decision reads O(1) rows.
3. **The single serialization point per campaign.** The decide-and-claim transaction must be serialized *per pool of agents* for the safety guarantee. One SQLite file makes that global — ~600 full calls/s on a laptop, flat from 1 to 8 workers (they queue on the one write lock). Fix: **shard by agent pool**, Postgres with `FOR UPDATE SKIP LOCKED`, one allocator per pool via a lease. Different pools never contend.
4. **Telecom limits** (carrier CPS, channels per trunk) are the real-world ceiling long before CPU.

The honest nuance: predictive gets *better* with bigger pools (variance grows like √n, so the safety margin shrinks relative to capacity), so you want the **largest** pool one allocator can serve, not the smallest that removes contention. And at 100,000, the first constraint is organizational — no single campaign has 100k agents, so pools map to campaigns/teams naturally.

**Do not answer "add more servers"** — the brief explicitly says that's the wrong answer. Name the bottleneck and why.

### Q. "Why did your algorithm decide to initiate 17 calls instead of 10?"

Run `python -m smartdialer.explain B --at 600` live if they let you — it prints every term. The structure of the answer:
- The **pacer** estimated: expected free agents = (free now) + (agents whose system-timed wrap-up ends within the answer horizon) + (live calls likely to end within the horizon), minus the answers it expects from calls *already ringing* (each weighted by its age-adjusted answer probability), divided by the answer rate `p`. That's the raw number.
- The **Safety Controller** then capped it: it computed its own capacity, took a conservative upper bound on `p`, modeled the number of answers as a Poisson-binomial over every in-flight call, and admitted new calls one at a time while `P(answers > capacity)` stayed under ε (2%). So "17" is "the most calls I can add such that the chance of more borrowers answering than I have agents is still under 2%."

So the answer is never a vibe — it's "here are the eight numbers that produced it."

### Q. "What part of your architecture are you least confident about?"

Be honest, pick a real one. Good candidates:
- **The single serialization point.** It's correct but it's the scaling ceiling; sharding by pool is designed but not implemented, and sharding trades away some of predictive's statistical advantage. That tension is the most interesting unsolved part.
- **The answer-rate model assumes within-window stationarity.** The drift rule and the Wilson bound cover the non-stationary case by falling back to progressive, but that costs utilization during transitions (you can see it in scenario D). A proper time-series/Bayesian estimate would react faster without over-reacting.
- **Outage-time abandons.** If a provider answers a call and then can't bridge it (dies between answer and bridge), that borrower is abandoned no matter what. Load-spreading bounds the blast radius to one provider's share; a hard per-provider cap on concurrent unbound calls would make it a fixed number. That's the one place the safety guarantee is "bounded" rather than "zero."

Picking a real weakness and having the fix ready is itself the thing they're testing.

---

## 2. Design decisions — "why this and not that"

### Why Python?
Correctness and clarity matter more than raw throughput here (the brief grades system design, concurrency and safety, not calls/sec). Python keeps the state machines and the safety math short enough to read and defend in an interview. **What it makes harder:** the GIL — so I use *process*-level concurrency (the load test is 8 OS processes) and keep hot paths O(1). A production worker could be rewritten in Go for a higher per-node ceiling without changing the design.

### Why SQLite, and why no Redis / Kafka / microservices?
The only hard requirement is *correct allocation under concurrency*. That's a transactional-integrity problem, and a relational DB with conditional updates and unique indexes solves it directly and provably. Adding Redis/Kafka would add a second source of truth (and the exact cache-vs-DB consistency bug the brief asks about) and more failure modes, without making the core guarantee stronger. SQLite in WAL mode gives real ACID transactions, a single serialized writer, partial unique indexes, and multi-process access with **zero setup** for the reviewer. Every guarantee I use (conditional UPDATE, unique index, `BEGIN IMMEDIATE`) exists identically in Postgres, which is the production target. **What it makes harder:** one DB is the throughput ceiling and a per-pool serialization point — covered under scale.

> If they push "isn't SQLite a toy?": it's the most-deployed database in the world and it's ACID. The constraint isn't ACID-correctness, it's write throughput on one file (single writer). That's exactly why the scale answer is "Postgres + shard by pool", and the code is written so that switch is mechanical.

### Why a transactional outbox instead of a message queue?
The state change and the intent to call the provider must be atomic — never dial without a record, never record without dialing. A broker would reintroduce the dual-write problem (DB says one thing, queue says another). The outbox (`commands` table, written in the same transaction, executed under a lease, idempotent on call id) gives at-least-once delivery with at-most-once *effect*, with one source of truth. **Harder:** it's polling, so up to one tick of latency, and dead commands need an ops surface (`status='DEAD'`).

### Why a statistical (Poisson-binomial) safety bound and not ML?
The brief says a sensible statistical approach is fine and that the safety boundary must be *defensible*. A closed-form bound is explainable in one screen, needs no training/serving infra, degrades gracefully (fall back to progressive when data is thin or weird), and gives a provable per-decision abandonment risk. ML would be a black box exactly where the compliance risk lives. **Harder:** exact Poisson-binomial is expensive at scale (fixed with a normal approximation) and it assumes a stationary answer rate within the window (covered by the drift rule + fallback).

### Why is wrap-up system-timed?
Because it turns near-future capacity from a *guess* into a *fact*. An agent whose wrap-up ends before the earliest plausible answer is real capacity you can bank with zero added abandon risk — and in the high-answer-rate scenario that's where most of predictive's advantage comes from. **Harder:** real wrap-up is agent-driven; production would use a max wrap-up with early-finish (the code already supports finishing early) and only bank wrap-ups ending very soon.

### Why spread calls across providers (the recent change)?
"Always use the first healthy provider" puts every in-flight call on one provider until it breaks — so if that provider answers calls it then can't bridge, *everyone* is abandoned. Round-robin across healthy providers bounds the blast radius of a single failure to its share. Measured: failover scenario holds ~71% utilization / 1.4% abandon through an outage, vs ~61% / 3.6% before. **Harder:** round-robin is load-oblivious; production would weight by health/latency and add a hard per-provider in-flight cap.

---

## 3. The safety boundary — why it *cannot* be bypassed

This is the highest-stakes claim in the whole submission. Be able to give the structural argument cold:

1. **The pacer has no capability to act.** `pacing.py` imports nothing that can write to the DB or touch a provider — there's a test (`test_safety.py::test_pacer_has_no_path_to_the_provider`) that parses its imports and asserts this. Its only output is an integer in a `PacingProposal`.
2. **Only the allocator inserts calls**, and it calls `SafetyController.authorize()` *itself*, inside its own write transaction, and inserts exactly `decision.approved` rows — never more.
3. **The decision and the inserts share one serialized transaction**, so two workers can't both spend the same headroom — the second worker's controller sees the first's calls already in flight (`test_two_workers_cannot_double_spend_predictive_headroom`).
4. **The provider is only ever contacted for rows in `calls`** (via the outbox), and a row not dialed within the 5 s permit TTL is cancelled — so a stale approval can't be cashed in later against changed capacity.
5. **The controller can't be turned off from outside:** it uses `__slots__` (can't attach `enabled=False`), its config is a *frozen* dataclass behind a *read-only property* (`test_safety_cannot_be_switched_off_from_outside`).

So even a buggy or adversarial pacer can only ever ask. The worst it can do is request a huge number and get it reduced/rejected with a logged reason.

---

## 4. Concurrency & distributed-systems deep cuts

- **CAS everywhere.** Every state transition is `UPDATE ... WHERE id=? AND state=<expected>` and the caller checks `rowcount`. An illegal or stale transition simply matches zero rows — no exceptions, no locks held across logic.
- **`BEGIN IMMEDIATE`** takes the write lock at the start of the transaction, so read-then-write logic (decide capacity, then claim) can't interleave with another writer → no lost updates, no write-skew.
- **Partial unique indexes** are the belt-and-braces: one active call per agent, one per borrower — impossible to commit a violation.
- **Idempotency** is layered: provider dial is idempotent on our call id; the event inbox PK `(provider, event_id)` dedupes webhook redeliveries; the outbox `UNIQUE(call_id, kind)` dedupes commands; and the forward-only call-state rank absorbs *semantic* duplicates (a second ANSWERED with a new event id).
- **Leases** (owner + `lease_until`, 2 s) mean a dead worker's in-progress command/claim is picked up by any other worker after the lease expires — that's the whole crash-recovery story, no special coordinator.
- **Maps to Postgres unchanged:** the `UPDATE ... WHERE state=?` is the same; add `FOR UPDATE SKIP LOCKED` in the agent sub-select so workers don't queue on the same "longest-idle" agent; shard by pool so independent campaigns never contend.

---

## 5. State machines — why they look like this

- **Call states are monotonic (ranked); terminal states are absorbing.** This one rule is what makes duplicate and out-of-order provider events harmless: a lower-or-equal-rank transition is rejected, anything after a terminal state is rejected. So `ANSWERED` after `CONNECTED`, `RINGING` after `ANSWERED`, anything after `COMPLETED` → no-op. You don't need per-event special-casing; the rank does it.
- **Why a separate `ANSWERED` vs `CONNECTED`?** Because in predictive mode the borrower can pick up *before* an agent is attached — `ANSWERED` with no agent is exactly the dangerous moment, and the 2 s grace + abandon logic hangs off it.
- **Why `ABANDONED` distinct from `FAILED`/`CANCELLED`?** Compliance. `ABANDONED` = the borrower answered and no agent reached them — that's the regulated event you must count and bound. `FAILED` = they never answered; `CANCELLED` = we stopped it before they could answer. The distinction is the whole point of the assignment.
- **Why is `CONNECTED → WRAP_UP` the only exit from `CONNECTED`?** An agent on a live call is never yanked away; a logout requested mid-call is deferred (`pending_state`) and applied when the call ends.

---

## 6. Predictive pacing — the math, term by term

```
H             = observed average dial→answer time
expected_free = free agents now
              + agents whose system-timed wrap-up ends within H          (a fact, not a guess)
              + Σ over live calls P(talk + wrap ends within H)           (memoryless: 1 − e^−(H−wrap)/talk)
in_flight     = Σ over already-ringing unbound calls of P(answer | still unanswered at its age)
need          = expected_free − in_flight
n             = need / p  × aggressiveness(abandon-feedback)   capped at 5× expected_free
```

Things to be ready to defend:
- **The age adjustment** `P(answer | unanswered at age a) = p·S(a)/(p·S(a)+1−p)`, where `S(a)` is the empirical fraction of answered calls that took longer than `a`. A call ringing 25 s when almost all answers come within 15 s is very unlikely to answer; counting it at full `p` wastes agents. This turned out to be a big source of the utilization gain.
- **Why divide by `p`?** To get a connect you must dial ~1/p numbers. If you need 7 more agents busy and 30% answer, you dial ~23.
- The pacer only *proposes*; the Safety Controller owns the actual risk decision with its own, more conservative numbers.

### Safety Controller specifics
- **capacity** = available − answered-waiting + (wrap-ups that certainly end before the earliest plausible answer + grace). Note it does **not** count talk-time endings — those are forecasts, and capacity must be conservative.
- **p_safe** = `max(Wilson upper bound short window, Wilson upper bound long window)` — conservative whether the rate is rising or falling.
- **The risk model** = Poisson-binomial over every in-flight unbound call (each with its own answer probability) + each candidate new call at `p_safe`. Admit calls while `P(answers > capacity) ≤ ε`. ε = 2% per decision, halved when the abandon rate passes 50% of the regulatory ceiling. Exact DP up to 400 free agents, normal approximation above.
- **Fallback to progressive** when: < 50 resolved attempts of history; abandon rate ≥ 80% of ceiling; anyone answered and waiting; or answer rate rising (drift z > 3).
- **Shed** when capacity collapses (agents vanish) and the risk for *already-ringing* calls exceeds 5% — cancel the newest unanswered ones (highest answer probability) until it's back in bounds.

---

## 7. Failure handling — one line each

- **Worker crash mid-setup:** lease expiry + idempotent re-dial (same call id → provider returns the existing call, borrower rings once). If the approval is stale (> 5 s), never re-dial — ask the provider (adopt if it exists, cancel if not). Events are handled by any other worker meanwhile.
- **Provider outage:** in-flight calls time out (setup/ring) to FAILED + hangup; held webhooks are redelivered late and absorbed; breaker opens after 8 samples ≥ 50% failure → new calls REJECT or fail over; borrowers get `system_failures` + exponential backoff with **no attempt charged**; recovers via half-open probes.
- **40 of 100 agents vanish in 3 s:** their setup calls are re-bound to other agents or cancelled immediately; next tick (≤ 1 s) the controller sees the new capacity → approvals go to 0 and the newest unanswered calls are shed.
- **Duplicate events:** inbox PK for redeliveries, rank rule for semantic dupes → one transition, one bridge.
- **Out-of-order events:** rank rule + absorbing terminals; a late ANSWERED on a dead call → hangup + counted honestly as an abandon.

All five are demonstrated step-by-step: `python -m smartdialer.demos`.

---

## 8. Testing & "Break → Fix" story

- **293 tests**: state machines, concurrency (threads *and* 8 processes), events, safety, the five failure cases, end-to-end sims, and a **randomized fuzzer**.
- **The fuzzer (`tests/test_chaos.py`)** runs 240 adversarial interleavings (reordered/duplicated/dropped events, worker crashes on both sides of the commit, agent churn) and asserts invariants after every step, then drains to a consistent terminal state.
- **It found a real bug:** call ids were generated from a per-worker *seeded* PRNG, so a restarted worker could regenerate the same id sequence and collide — the `UNIQUE(calls.id)` constraint then aborted the allocation transaction. Fixed with truly-random `uuid4` ids. This is a great story to tell: "I didn't just test the happy path; I fuzzed the correctness boundary and it caught a restart bug that hand-written tests missed."

If asked **"how do you know it's correct?"**: "An invariant checker (`invariants.py`) that asserts the core safety properties — no agent or borrower with two active calls, agent↔call back-references consistent, bound calls ≤ agents — runs every 10 simulated seconds in every scenario and after every step of 240 fuzz runs. Zero violations."

---

## 9. Likely curveballs & how to answer

- **"What if the provider never sends COMPLETED?"** The reconciler closes it: ring timeout (45 s), setup timeout (20 s, with a provider lookup first), max call length, and it releases the agent and settles the borrower.
- **"What if two workers both run the reconciler?"** Fine — every reconciler action is a CAS, so the loser matches zero rows. Any worker can run any housekeeping.
- **"Isn't `BEGIN IMMEDIATE` a global lock that kills throughput?"** Yes, per database — that's the measured ceiling and the reason the scale plan shards by pool with Postgres row locks. Correctness first; the lock is why two workers can never double-spend headroom.
- **"Why not just cap concurrent calls at N and skip the stats?"** That's progressive with extra steps; it either leaves agents idle (cap too low) or abandons people (cap too high). The whole value is adapting the cap to the live answer rate *with a provable abandon bound*.
- **"Your abandon rate is 0.00% — is predictive even doing anything?"** It's connecting 1.1–1.6× as many borrowers as progressive with the same agents; 0% abandon means the bound is conservative, which is the correct bias for a compliance setting. The ε knob trades a little more utilization for a little abandon risk if the business wants it.
- **"What's ε and who sets it?"** The per-decision overflow-risk budget (default 2%). It's the business/compliance dial: ε = 0 is exactly progressive; a small ε buys most of predictive's gain for a bounded, auditable abandon risk.

---

## 10. If they make you live-code / change something

The code is deliberately factored so common asks are small, local changes — know where each lives:
- **"Change the wrap-up time / grace / timeouts"** → `config.py` (`DialerConfig`), one dataclass.
- **"Make the abandon ceiling stricter"** → `safety.py` (`SafetyConfig.max_abandon_rate` / `risk_epsilon`).
- **"Add a new call outcome / state"** → `states.py` (add to the enum + `CALL_RANK`), then handle it in `events.py`.
- **"Add a third provider / change routing"** → implement `providers/base.TelecomProvider`; routing is `providers/router.py::choose`.
- **"Add a new failure scenario"** → `sim/scenarios.py` (a `script` entry), run with `-m smartdialer.sim`.
- **"Why did this specific decision happen?"** → `python -m smartdialer.explain`, or read the `safety_decisions` table (every decision is logged with its full inputs).
- **Debugging exercise prep:** the most likely injected bug is in an event transition or a CAS `WHERE` clause — re-run `pytest -q tests/test_events.py` and the fuzzer, and lean on `invariants.check()` to localize it.

---

## 11. "What would you do with another week?"

In priority order (this answer shows judgment):
1. **Incremental snapshot counters** — the real scaling bottleneck; makes each decision O(1) reads instead of scanning 15–30 min of calls.
2. **Shard the serialized commit by agent pool** (Postgres + `SKIP LOCKED`) so it scales past ~1,000 agents, while keeping pools as large as one allocator can serve (predictive likes big pools).
3. **A hard per-provider cap on concurrent unbound calls** — turns outage-time abandon exposure from "a share" into a fixed number.
4. **A better answer-rate estimator** (Bayesian / time-series) so transitions cost less utilization than the current fall-back-to-progressive.
5. **Wire a live Plivo adapter** behind the existing interface (the "cherry on the cake"; needs an account).

---

## 12. Code map (so you can navigate fast under pressure)

| Concept | File | Key function(s) |
|---|---|---|
| State definitions, the rank rule | `states.py` | `CALL_RANK`, `call_transition_allowed`, `AGENT_TRANSITIONS` |
| DB schema, transactions, CAS helpers | `store.py` | `Store.tx` (`BEGIN IMMEDIATE`), unique indexes |
| Core transitions | `calls.py` | `claim_available_agent`, `connect_answered`, `finalize`, `rebind_or_cancel` |
| Campaign snapshot + age-adjusted answer prob | `snapshot.py` | `take_snapshot`, `p_answer_given_age` |
| Pacing (proposes a number only) | `pacing.py` | `ProgressivePacer`, `PredictivePacer.propose` |
| Safety Controller (authorizes) | `safety.py` | `SafetyController.authorize`, `OverflowRisk`, `shed_target` |
| Allocator + outbox dispatcher | `allocator.py` | `CallAllocator.allocate`, `dispatch`, `_exec_dial` |
| Provider event ingestion | `events.py` | `EventIngestor.ingest`, `_apply` |
| Agent presence | `agents.py` | `AgentService.go_unavailable` (the mid-setup case) |
| Timers / convergence | `reconciler.py` | `Reconciler.run` |
| Worker loop | `worker.py` | `DialerWorker.tick` |
| Invariants | `invariants.py` | `check` |
| Providers + breaker + spreading | `providers/` | `mock.py`, `router.py::choose` |
| Simulator / scenarios / CLIs | `sim/`, `demos.py`, `explain.py` | — |

### Numbers worth memorizing
- Utilization lift (30 min, 100 agents): A 43%→69%, B 63%→80%, C 84%→88%, D 55%→68%. Abandon 0.00%, invariant violations 0.
- Throughput: ~600 full calls/s on one SQLite file, flat across 1–8 workers (single writer).
- Safety math: exact 190 ms at 1k agents, 27 s at 10k → normal approx 3 ms at 10k.
- Defaults: wrap-up 10 s, abandon grace 2 s, permit TTL 5 s, command lease 2 s, heartbeat timeout 15 s, breaker opens at 8 samples ≥ 50% fail, ε = 2%.
- 293 tests; fuzzer 240 runs; it found the call-id-collision bug.
