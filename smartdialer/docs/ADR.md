# Architecture decision record

The brief asks four questions about every choice: what, why, what it solves, what it makes harder.

## ADR-1: Python

* **What.** Python 3.11+, standard library only (sqlite3, dataclasses, threading, multiprocessing). No third-party runtime dependencies; pytest for tests.
* **Why.** The assignment is graded on correctness of allocation, the safety boundary, and clear reasoning — not raw throughput. Python keeps the state machines, the pacing maths and the safety logic short and readable, which is the thing I have to be able to walk through and defend. The discrete-event simulator and the real dialer share one clock interface, so every scenario exercises the real code.
* **Solves.** Fast to build and reason about in the timebox; zero-setup for the reviewer (`python -m smartdialer.sim` just runs).
* **Harder.** The GIL means a single process won't use many cores, and per-operation overhead is higher than Go/Rust. I lean on *process*-level concurrency (the load test runs 8 OS processes against one DB) and keep hot paths (the risk DP) O(1)-friendly. A production rewrite of the worker in Go would raise the per-node ceiling, but wouldn't change the design.

## ADR-2: SQLite (WAL) as the single source of truth — no Redis, Kafka, or services

* **What.** One SQLite database in WAL mode holds agents, borrowers, calls, the command outbox, the provider-event inbox and the decision log. Every state change is a compare-and-set `UPDATE ... WHERE id=? AND state=?`; the decide-and-claim step runs in a `BEGIN IMMEDIATE` transaction. Workers are identical, stateless processes.
* **Why.** The only hard requirement in this problem is *correct allocation under concurrency*: two workers must never reserve the same agent, and the predictive path must never commit more calls than the safety bound allows. That is a transactional-integrity problem, and a relational DB with conditional updates and partial unique indexes solves it directly and provably. Adding Redis or Kafka would add moving parts, a second source of truth (and the cache-vs-DB consistency question the brief explicitly asks about), and failure modes — without making the core guarantee any stronger.
* **Solves.** Atomic reserve (CAS + unique index); crash recovery with no special machinery (leases + timers, run by any worker); exactly-once event handling (inbox PK + forward-only states); exactly-once side effects (transactional outbox); and one auditable place for every decision.
* **Harder.** One database is a throughput ceiling and, for the safety guarantee, a per-pool serialisation point. `docs/ARCHITECTURE.md` §9 measures it (~600 full calls/s, ~4,000 write tx/s on a laptop, flat across workers) and gives the fix: move to PostgreSQL with `FOR UPDATE SKIP LOCKED`, and **shard by agent pool** so independent pools never contend. The code is written so that switch is mechanical — the CAS statements and the `BEGIN IMMEDIATE` block are already exactly what Postgres wants. SQLite's single-writer also means I can't scale writes within one file; that's acceptable up to ~1,000 agents and is the explicit boundary of the prototype.

## ADR-3: Transactional outbox instead of a message broker

* **What.** Side effects on the provider (DIAL/BRIDGE/HANGUP) are rows in a `commands` table written in the same transaction as the state change, then executed under a short lease by any worker, idempotently (our call id is the provider's idempotency key).
* **Why.** The state change and the intent to act must be atomic: we must never dial without a record, nor record without dialling. A broker would be a second system to keep consistent with the DB (the classic dual-write problem). The outbox keeps one source of truth and still gives at-least-once delivery with at-most-once effect.
* **Solves.** Crash between "decided to dial" and "dialled"; duplicate dials after retries; torn-down calls after we give up.
* **Harder.** Executing commands is polling, not push, so there's up to one tick of latency, and a dead-letter (`status='DEAD'`) needs an ops surface. At scale the outbox is partitioned by call id; that's straightforward because commands are independent.

## ADR-4: Statistical (Poisson-binomial) safety bound, not an ML pacer

* **What.** The pacer is a transparent formula (expected free agents ÷ answer rate, age-adjusted). The Safety Controller models "how many in-flight calls will be answered" as a Poisson-binomial and admits calls only while `P(answers > capacity) ≤ ε`, using a conservative Wilson upper bound for the answer rate.
* **Why.** The brief says a sensible statistical approach is fine and that the safety boundary must be defensible. A closed-form bound can be explained in one screen (`python -m smartdialer.explain`), has no training/serving infrastructure, degrades gracefully (fall back to progressive when data is thin or weird), and gives a provable per-decision abandonment risk. An ML model would be a black box exactly where the compliance risk lives.
* **Solves.** "Why 17 and not 10?" and "why did you only allow 3?" both have itemised answers; a hard, tunable ceiling on expected abandons.
* **Harder.** Exact Poisson-binomial is O(in-flight × capacity) and became the first scaling bottleneck — found via the load test, fixed with a normal approximation above 400 free agents (matches exact to 3 decimals at the 2% tail). The model assumes within-window stationarity of the answer rate; the drift rule and the `max(UCB)` guard cover the non-stationary case by falling back, at a cost in utilization during transitions (visible in scenario D).

## ADR-5: Spread new calls across healthy providers

* **What.** The provider router sends new calls round-robin across every CLOSED provider rather than always to the first one, keeping the circuit breaker and half-open probes.
* **Why.** "First healthy provider" puts every in-flight call on one provider until it breaks. If that provider then answers calls it can't bridge (sudden death between answer and bridge), every one of those borrowers is abandoned. Spreading bounds the blast radius of a single provider failure to its share of traffic.
* **Solves.** `outage_failover` holds ~71% utilization / 1.4% abandon through a 2-minute single-provider outage, versus ~61% / 3.6% with all-on-one routing.
* **Harder.** Round-robin is load-oblivious (it ignores that providers may have different capacities or latencies); a production version would weight by health/latency and add a hard per-provider cap on concurrent unbound calls. It also means a call's provider is now less predictable, which is why the provider is pinned on the call row at first dial and every retry honours it.

## ADR-6: System-timed wrap-up

* **What.** Wrap-up ends on a stored timer (`wrap_until`), not on an agent action.
* **Why.** It turns near-future capacity from a guess into a fact the Safety Controller can bank (an agent whose wrap-up ends before the earliest plausible answer is real capacity now), which is a meaningful, *safe* source of the predictive gain.
* **Solves.** Tighter, still-safe pacing without assuming anything about talk times.
* **Harder.** Real wrap-up is agent-driven and variable; in production this becomes "max wrap-up time" with the agent able to finish early (`go_available` already supports finishing early), and the controller should only bank wrap-ups expiring very soon.

## ADR-7: Fuzz the correctness boundary, don't just unit-test it

* **What.** Alongside ~50 targeted tests, a randomized property/fuzz test (`tests/test_chaos.py`, 240 seeds) hammers the allocation and event paths with reordered/duplicated/dropped events, worker crashes, and agent churn, asserting invariants after every step and full drain-consistency at the end.
* **Why.** The rubric's top line is "correctness before cleverness ... a beautiful dashboard doesn't matter if two workers can allocate the same agent." Hand-picked inputs can't cover the combinatorial space of interleavings that actually breaks distributed state machines.
* **Solves.** It found a real call-id collision bug on worker restart that every unit test had missed (see ADR note and `docs/ARCHITECTURE.md` §8a). It is also a living regression net for the invariants.
* **Harder.** Fuzz failures need a reproducer; each case prints its `seed` and `mode` so any failure replays deterministically with `pytest -k "[<seed>]"`.
