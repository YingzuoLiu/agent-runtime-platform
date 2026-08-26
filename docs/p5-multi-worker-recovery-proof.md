# P5 multi-worker concurrency and recovery proof

P5 is a required PostgreSQL integration gate for the accepted application-authority
baseline. It starts two independent Python worker processes, each with one
`RuntimeManager` and a stable owner ID, against one isolated PostgreSQL schema. A
controller process submits Runs, controls named one-shot barriers, injects exact
store-time lease expiry, and rereads durable evidence. The synthetic HTTP provider is
a fourth, independent process with its own SQLite effect ledger.

Each reusable barrier has monotonic, lock-free armed, consumed, reached, released,
and completed generation counters. Every counter has one writer, so the deliberate
SIGSTOP schedule cannot freeze a process-shared generation mutex. Events are wake-up
hints only: a stale event cannot satisfy a different generation, and the controller
cannot re-arm until the prior generation's consumer has acknowledged completion.
Starting a new schedule first releases every
prior named barrier, so a worker parked on one hook cannot prevent it from reaching a
different hook in the next schedule. Arming the new schedule is also serialized with
the proof adapter's whole `claim_next_run` cycle. A poll already between
`claim.before` and `claim.result` must finish before either new hook becomes visible,
so one call cannot miss the new before generation and consume its result generation.
Timeout diagnostics request locals-free worker thread stacks and integer-only
generation state so a failed barrier identifies the exact blocking boundary without
publishing credentials or payloads. Failure cleanup resumes a stopped proof worker
and has a bounded SIGKILL backstop. The post-run PostgreSQL session check runs only
after both polling workers have stopped; an in-flight polling transaction is not
misclassified as a leaked session.

The proof adds no production scheduler, distributed queue, connection pool, or
production control endpoint. `RuntimeManager._wake` remains a process-local latency
optimization; bounded PostgreSQL polling is the durable progress mechanism. Fault
hooks exist only in the proof worker adapter and accept no arbitrary code, SQL,
credentials, DSNs, lease tokens, or dispatch tokens.

## Production correction under proof

P5 makes one behavioral correction to `runtime_service/run_store.py`, driven by real
S7 evidence rather than by inspection. Everything else here is proof scaffolding.

`pg_terminate_backend` makes PostgreSQL report SQLSTATE `57P01` (`AdminShutdown`) on
the terminated session. `is_run_store_retryable_error` now admits `57P01` at the
existing bounded polling boundary in `RuntimeManager._worker_loop`, which runs before
any Runtime or provider code is invoked. Neighbouring `57P02` (`CrashShutdown`) is
deliberately not admitted, and `57P01` is not classified as contention.

That boundary is retry-safe but not literally read-only: `claim_next_run` writes. A
claim that commits without returning its result strands a lease that expires on the
server's own deadline and is recovered exactly once, as S4 proves. No provider effect
is replayed.

Admitting the state is not sufficient on its own. The classifier also carried a
driver-class fallback that treated any Psycopg error *named* `OperationalError` or
`InterfaceError` as retryable, whatever SQLSTATE it reported. Psycopg normally raises
a SQLSTATE-specific subclass, so that fallback did not mask the missing `57P01` -- but
it did leave the allowlist unenforceable in the other direction, because an excluded
state arriving as a bare `OperationalError` would retry anyway. The fallback is now
restricted to errors carrying no SQLSTATE, which is the client-side connectivity case
it was written for. When the server reports a state, that state decides.

The two halves are gated differently, and deliberately so:

| Correction | Gate | Why |
|---|---|---|
| `57P01` is admitted | `P5M09` under S7, plus unit tests | S7 terminates a real backend, so a mutant that removes the state kills the scenario |
| A reported SQLSTATE outranks the driver class name | Unit tests only | No S1-S8 schedule can observe it: the previous behaviour was strictly more permissive, so it cannot fail a passing scenario |

## Run the required gate

Provide a PostgreSQL 16 DSN only through the environment. The command fails rather
than skips when the DSN or required process control is unavailable.

```bash
P5_POSTGRES_DSN='postgresql://...' \
  python examples/p5_multi_worker_proof.py
```

The controller creates and drops one `p5_*` schema. On success it writes the live,
sanitized report to `artifacts/p5-multi-worker-proof.json`. Live artifacts are not
committed; the deterministic contract fixture is
`examples/p5-multi-worker-proof.example.json`.

For a bounded mutation sample, run:

```bash
P5_POSTGRES_DSN='postgresql://...' \
  python tests/conformance/p5_mutation_proof.py
```

The mutation runner changes and restores source around every mutant. It requires all
nine representative faults to fail their intended semantic assertion: duplicate
live ownership, wake-only progress, same-Thread overlap, global serialization,
pre-expiry takeover, stale Memory mutation, unsafe replay, DSN leakage, and a store
retry allowlist that no longer admits the terminated-connection SQLSTATE.

## Scenario contract

| ID | Deterministic transition | Required durable postcondition |
|---|---|---|
| S1 | Both processes reach `claim.before`; fixed winner is released first | One live owner, one `run.started`, only the owner executes |
| S2 | First same-Thread Run is held at execution | Second remains queued; checkpoint bases advance from 0 to 1 |
| S3 | Different-Thread Runs are held in both processes before either release | Both Runs are concurrently live; an idle cross-process submission is later found by polling |
| S4 | Owner is SIGKILLed at `checkpoint.commit_pending` | No live-lease theft; exact server-time expiry yields attempt 2 and one terminal checkpoint |
| S5 | Old owner is SIGSTOPed; replacement persists live authoritative progress | Resumed old token is rejected by Run, Workflow, Memory, mirror, and action mutations |
| S6 | Provider effect is committed and response is withheld before SIGKILL | Idempotent path reuses one request identity/effect; unsafe path is not replayed; known controls agree |
| S7 | `pg_terminate_backend` breaks the worker's real claim connection | Observed SQLSTATE is exactly `57P01`; retry is bounded; restart protects the live lease; exact expiry recovers once |
| S8 | Four fixed `a-first`/`b-first` schedules | Both release classes and expected winners are recorded; every Run uses attempt 1 |

S5 deliberately resumes A while B still owns a live attempt-2 lease. This distinguishes
lease-token fencing from a weaker terminal-status rejection. The sampled matrix covers
Run events/terminal commit, Workflow events, governed Memory snapshot/mutation,
evidence mirrors, and external-action dispatch. Existing PostgreSQL execution-plane
and Memory mutation suites remain the exhaustive store-method backstop.

## Report and disclosure boundary

The report version is `p5-multi-worker-recovery:1`. It includes code head/tree,
PostgreSQL and schema versions, two stable worker IDs, S1-S8 correlations and
postconditions, S8 schedule coverage, and pass/fail totals. The proof and unit tests
reject seeded DSNs/passwords, authorization material, internal lease/dispatch-token
shapes, raw provider responses, and unbounded tracebacks. Provider evidence exposes
only counts, stable-identity counts, and allowlisted event types.

The claim is bounded: two independent worker processes make safe progress against one
PostgreSQL authority under deterministic contention, crash, stale-writer, ambiguous
effect, and connection-loss schedules. It is not a claim of high availability,
exactly-once effects, distributed-queue semantics, production scale, cloud readiness,
or zero-downtime operation. P6 remains responsible for AWS deployment concerns.

## Recorded for P6, not changed here

`RuntimeManager._heartbeat_loop` treats any exception from `renew_run_lease` as a
failed renewal and latches that attempt stale. This is fail-safe -- the Run is
recovered once after lease expiry and no second owner executes concurrently -- but it
is the opposite policy from the claim boundary above: the same PostgreSQL restart the
claim loop now survives still abandons every in-flight Run on that worker. S7 does not
reach this path, because it terminates the connection before a claim's first store
query rather than during execution.

This is deliberately left unchanged in P5. A retryable renewal would have to be
bounded strictly inside the remaining lease window or it weakens the fencing S5
proves, and it needs its own scenario and mutant. P6 owns it.

After this PR is independently reviewed and merged, update the Living PRD with the
accepted merge commit, exact CI evidence, P5 status `ACCEPTED`, and P6 as the current
phase. Do not advance that baseline from an unmerged feature head.
