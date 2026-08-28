# Documentation index

The root [`README.md`](../README.md) is the public overview. This index distinguishes current
operational contracts and executable proof records from earlier capability-milestone documents.
When two documents appear to disagree about deployment readiness, the current operational set and
the accepted code baseline take precedence.

## Current scope and operational contracts

- [`current-scope-and-limitations.md`](current-scope-and-limitations.md) — supported claims,
  non-claims, and the evidence needed to expand them.
- [`portable-substrate-contract.md`](portable-substrate-contract.md) — provider-neutral boundary
  and the checks that prevent cloud authority from leaking into Runtime Core.
- [`production-runtime-contract.md`](production-runtime-contract.md) — image, configuration,
  health, log, signal, identity, and PostgreSQL authority contract.
- [`postgresql-application-composition.md`](postgresql-application-composition.md) — coherent
  backend selection, schema bootstrap/startup authority, connection budgets, and readiness.
- [`postgresql-store-backend.md`](postgresql-store-backend.md) — PostgreSQL Run/Workflow schema,
  transaction, leasing, conformance, and rollback boundary.
- [`postgresql-memory-store.md`](postgresql-memory-store.md) — PostgreSQL Memory authority,
  version order, audit transactions, conformance, and mutation proof.
- [`store-semantic-conformance.md`](store-semantic-conformance.md) — shared SQLite/PostgreSQL
  invariant-to-test matrix.
- [`durable-run-leasing.md`](durable-run-leasing.md) — worker ownership, heartbeat, expiry,
  takeover, fencing, and migration boundary.
- [`thread-execution-serialization.md`](thread-execution-serialization.md) — tenant-qualified
  ordering and checkpoint revision compare-and-swap.
- [`state-boundaries.md`](state-boundaries.md) — persisted state ownership and Thread projection.
- [`operator-quarantine-resolution.md`](operator-quarantine-resolution.md) — controlled dry-run,
  apply, stale-plan rejection, and evidence preservation.
- [`durable-external-actions.md`](durable-external-actions.md) — external-action ledger,
  idempotency, cancellation arbitration, and uncertain outcomes.
- [`durable-action-gateway.md`](durable-action-gateway.md) — bounded public Action API and
  server-owned destination configuration.
- [`governed-memory.md`](governed-memory.md) — subject isolation, sealed snapshots, forgetting,
  RBAC, and audit evidence.

The provider-specific AWS adapter and its explicit no-live-AWS boundary are documented in
[`deploy/aws/README.md`](../deploy/aws/README.md).

## Executable proof records

- [`p5-multi-worker-recovery-proof.md`](p5-multi-worker-recovery-proof.md) — independent
  PostgreSQL workers, recovery, fencing, ordering, and mutation evidence.
- [`p6b2-exact-image-proof.md`](p6b2-exact-image-proof.md) — digest-identified production image,
  PostgreSQL TLS verification, negative configuration, secret-canary, and shutdown evidence.
- [`action-recovery-proof.md`](action-recovery-proof.md) — real HTTP-sidecar restart proof for
  safe replay and explicit unknown outcomes.

These records prove their bounded scenarios. They do not turn a local or CI topology into a live
multi-host, HA, or AWS deployment claim.

## Capability and milestone records

These documents describe implemented behavior and remain valid within their stated scopes. Their
phase-local deployment summaries may predate the P4-P6 operational program, so they are not the
authority for current production readiness.

- [`dynamic-tool-loop.md`](dynamic-tool-loop.md) — Planner decisions, policy order, failure codes,
  replay, and model adapter.
- [`release-validation-workflow.md`](release-validation-workflow.md) — deterministic DAG,
  selective replay, signatures, and interrupted recovery.
- [`evidence-review-workflow.md`](evidence-review-workflow.md) — review contracts, partial results,
  replanning, and semantic-analyzer boundary.
- [`bring-your-own-domain.md`](bring-your-own-domain.md) — trusted startup-time extension seam and
  incident-triage example.
- [`cloud-runtime.md`](cloud-runtime.md) — historical Phase 3-7D architecture, API, security, and
  deployment evolution.
- [`sample_trace.md`](sample_trace.md) — annotated application/runtime execution trace.

Historical reinforcement-learning experiments are separately labelled in [`rl/`](../rl/). The
maintained deterministic evaluation harness remains in [`eval/`](../eval/).
