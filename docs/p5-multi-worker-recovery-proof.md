# P5 multi-worker concurrency and recovery proof

P5 is a required PostgreSQL integration gate for the accepted application-authority
baseline. It starts two independent Python worker processes, each with one
`RuntimeManager` and a stable owner ID, against one isolated PostgreSQL schema. A
controller process submits Runs, controls named one-shot barriers, injects exact
store-time lease expiry, and rereads durable evidence. The synthetic HTTP provider is
a fourth, independent process with its own SQLite effect ledger.

The proof adds no production scheduler, distributed queue, connection pool, or
production control endpoint. `RuntimeManager._wake` remains a process-local latency
optimization; bounded PostgreSQL polling is the durable progress mechanism. Fault
hooks exist only in the proof worker adapter and accept no arbitrary code, SQL,
credentials, DSNs, lease tokens, or dispatch tokens.

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
eight representative faults to fail their intended semantic assertion: duplicate
live ownership, wake-only progress, same-Thread overlap, global serialization,
pre-expiry takeover, stale Memory mutation, unsafe replay, and DSN leakage.

## Scenario contract

| ID | Deterministic transition | Required durable postcondition |
|---|---|---|
| S1 | Both processes reach `claim.before`; fixed winner is released first | One live owner, one `run.started`, only the owner executes |
| S2 | First same-Thread Run is held at execution | Second remains queued; checkpoint bases advance from 0 to 1 |
| S3 | Different-Thread Runs are held in both processes before either release | Both Runs are concurrently live; an idle cross-process submission is later found by polling |
| S4 | Owner is SIGKILLed at `checkpoint.commit_pending` | No live-lease theft; exact server-time expiry yields attempt 2 and one terminal checkpoint |
| S5 | Old owner is SIGSTOPed; replacement persists live authoritative progress | Resumed old token is rejected by Run, Workflow, Memory, mirror, and action mutations |
| S6 | Provider effect is committed and response is withheld before SIGKILL | Idempotent path reuses one request identity/effect; unsafe path is not replayed; known controls agree |
| S7 | `pg_terminate_backend` breaks the worker's real claim connection | Retry is classified and bounded; restart protects the live lease; exact expiry recovers once |
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

After this PR is independently reviewed and merged, update the Living PRD with the
accepted merge commit, exact CI evidence, P5 status `ACCEPTED`, and P6 as the current
phase. Do not advance that baseline from an unmerged feature head.
