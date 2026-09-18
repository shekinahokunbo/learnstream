# Measured results

Run against the deployed stack in `us-east-1` on 18 September 2026 with
`python3 scripts/measure.py`. Raw output is in `results/summary.txt`
(gitignored); these are the same numbers.

| | result |
|---|---|
| Throughput | **38.4 events/second** — 6,898 requests, **0 failures** |
| Latency | **p50 70 ms · p95 140 ms · p99 160 ms** |
| Load shape | 15 concurrent producers, 3 minutes |
| Idempotency | **25 deliveries of one event id → 1 stored** |
| Fault injection | **50 / 50 events recovered** after a dead-letter-queue redrive |
| Test coverage | **98%** (166 statements, 4 missed), 23 tests |

Throughput here is bounded by the AWS account, not by the design: this
account's Lambda concurrency pool is **10**, where the AWS default is 1000.
An earlier run at 50 concurrent producers reached 126 requests/second with
25% of requests throttled — see below.

## What deploying it taught me

Four bugs survived a green test suite and only appeared against live AWS.

**1. Reserved concurrency cannot be reserved from an empty pool.**
The worker asked for `reservedConcurrentExecutions=20`. AWS requires at least
10 unreserved, and this account's whole pool is 10, so the stack refused to
create. Reserving is also the wrong tool: it takes capacity out of the
account-wide pool. The cap now lives on the SQS event source
(`maxConcurrency`), which bounds consumer fan-out without reserving anything.

**2. Concurrency starvation looks like a server error.**
Under 50 producers, 25% of requests returned HTTP 500 — including requests
that should have been rejected as 400. CloudWatch showed **5,681 throttles
and 0 errors** on the ingest function: the asynchronous consumer was eating
the concurrency the synchronous front door needed. Moving the consumer cap
down and re-measuring inside the budget gave zero failures.

**3. The idempotency key was on the wrong attribute.**
The event sort key was `EVENT#<occurred_at>#<event_id>`. A producer that
retries without supplying its own `occurred_at` gets a fresh server-side
timestamp, so **25 replays of one event id stored 5 events**. Every unit test
passed, because every test used one fixed timestamp. Re-keyed to
`EVENT#<event_id>`; a regression test now replays with three different
timestamps and asserts a single stored event.

**4. A drill can measure on the wrong clock.**
The queue's visibility timeout is 6x the worker timeout (180s), which is the
recommended setting. That means three failed receives take more than nine
minutes to reach the dead-letter queue, while the drill waited 200 seconds
and reported zero. The drill now shortens the visibility timeout for its own
duration and restores it in a `finally` block.

## Reproducing

```bash
./run_it.sh                              # tests, deploy, measure
python3 scripts/measure.py --skip-load   # re-measure without re-running load
python3 scripts/diag_errors.py           # throttles vs errors, concurrency budget
```
