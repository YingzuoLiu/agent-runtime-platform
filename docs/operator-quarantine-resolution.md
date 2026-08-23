# Operator quarantine resolution

The Runtime can detect a checkpoint revision conflict while a recovered Run still has
external-action precedence. In that case it cannot execute against stale state, overwrite the
newer checkpoint, or hide a potentially real external effect behind an ordinary terminal failure.
It therefore keeps the Run `running`, clears its lease, records
`thread_checkpoint_conflict_reconciliation_pending`, and blocks the same tenant-qualified Thread.

That fail-closed quarantine remains the default. This capability adds one narrow, operator-only
way to release it after durable evidence proves that no provider reconciliation remains:

```text
terminalize_failed_preserving_checkpoint
```

The command fails the old Run, preserves the authoritative checkpoint and every workflow/tool/
external-action record, appends audit evidence, and releases the Thread slot. It does not resume,
retry, rebase, roll back, merge, or repair the old execution.

## Why this is a controlled command

The design follows two useful production patterns without copying either mechanism:

- [Kubernetes finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
  keep an object blocked until named preconditions are satisfied and warn against blindly removing
  the blocker.
- [AWS Step Functions redrive](https://docs.aws.amazon.com/step-functions/latest/dg/redrive-executions.html)
  checks eligibility, preserves successful history, and appends new audit events.

This Runtime has a stronger conflict than ordinary redrive: its checkpoint moved and an external
effect may exist. The first resolution therefore never reruns a step. All eligibility and mutation
logic is deterministic; no LLM participates.

## Command surface and permission

The existing FastAPI authentication boundary exposes:

```text
POST /operator/quarantine-resolutions
permission: quarantine:resolve
```

The default Viewer role does not have this permission. The default Operator role does. Lookup is
tenant-scoped before authorization: an unknown target and a cross-tenant target return the same
`404 quarantine_target_not_found`, while a same-tenant Viewer receives
`403 operation_not_permitted`.

A public Run is targeted by `run_id`:

```json
{
  "target": {"run_id": "run_123"},
  "resolution": "terminalize_failed_preserving_checkpoint",
  "dry_run": true
}
```

A private Durable Action is targeted by its public `action_id`:

```json
{
  "target": {"action_id": "run_action_123"},
  "resolution": "terminalize_failed_preserving_checkpoint",
  "dry_run": true
}
```

Exactly one target field is allowed. A private Action-owned Run remains unavailable through generic
Run routes. Its plan uses `action_id` as the public Thread-slot reference and never exposes the
private Run's Thread ID.

## Eligibility

Every condition below must hold in one consistent SQLite snapshot:

1. the tenant-scoped target is a public Run or a valid private Action target;
2. the caller has `quarantine:resolve`;
3. the Run is still `running` with
   `thread_checkpoint_conflict_reconciliation_pending`;
4. lease owner, lease token, heartbeat, and expiry are all cleared;
5. a `checkpoint.conflict` event has disposition
   `external_action_reconciliation_quarantined`;
6. the event's expected revision equals `checkpoint_base_revision`;
7. its observed revision equals the current Thread checkpoint revision;
8. the base and current revisions still differ;
9. a workflow execution and at least one external-action record exist;
10. every external action has durable terminal row, parent-tool, and terminal-event evidence;
11. workflow/action evidence is internally consistent and unchanged from the plan;
12. Run attempt, cancellation flag, quarantine marker, checkpoint content/version, and lease state
    are unchanged from the plan.

Allowed terminal external-action states are:

```text
succeeded
failed
outcome_unknown
```

`outcome_unknown` is not a guess that the provider succeeded or failed. It is an explicit durable
terminal safety conclusion: the Runtime will not redispatch the uncertain effect. By contrast,
`prepared`, `dispatching`, and the public Action projection `reconciling` remain ineligible and keep
the Thread blocked.

Current stable ineligibility reason codes are:

```text
run_not_quarantined
execution_authority_present
checkpoint_conflict_event_missing
checkpoint_conflict_event_mismatch
checkpoint_revision_not_drifted
workflow_execution_missing
external_action_evidence_missing
external_action_nonterminal
external_action_evidence_inconsistent
```

An ineligible dry-run returns `200` with `eligible=false`, no plan ID, `no change`, and
`remains_blocked`. Apply never has a force or ignore option.

## Typed plan and plan ID

An eligible dry-run returns a sanitized plan like:

```json
{
  "outcome": "dry_run",
  "reused": false,
  "verified": false,
  "plan": {
    "target": {"run_id": "run_123", "action_id": null},
    "resolution": "terminalize_failed_preserving_checkpoint",
    "eligible": true,
    "plan_id": "qrp_<sha256>",
    "thread": {
      "tenant_id": "tenant-a",
      "reference_kind": "thread_id",
      "reference": "thread-42"
    },
    "current_run_status": "running",
    "current_quarantine_code": "thread_checkpoint_conflict_reconciliation_pending",
    "checkpoint_base_revision": 1,
    "observed_checkpoint_revision": 2,
    "external_actions": {
      "total": 1,
      "prepared": 0,
      "dispatching": 0,
      "succeeded": 1,
      "failed": 0,
      "outcome_unknown": 0
    },
    "workflow_reconciliation_required": false,
    "planned_run_transition": "running -> failed",
    "checkpoint_disposition": "preserved",
    "external_evidence_disposition": "preserved",
    "provider_calls": 0,
    "new_audit_events": ["quarantine.resolution_applied", "run.failed"],
    "thread_disposition": "released_after_atomic_commit",
    "ineligibility_reasons": []
  }
}
```

The opaque `qrp_` ID hashes a canonical precondition snapshot. It covers tenant, target and
resolution kind, Run status/attempt/cancellation/quarantine version, lease-cleared state, conflict
event identity, checkpoint revision plus an opaque content fingerprint, the complete persisted
workflow/tool/action/event evidence, and the external-action status summary. Private evidence is
used only inside the composite hash input; no individual field hash or raw value is returned.

The plan and audit event never contain checkpoint JSON, Run state, tool arguments, provider result
bodies, lease or dispatch tokens, caller/provider idempotency keys, credentials, provider identity,
or raw exception chains. The plan ID is a stale-state detector, not an authority token; every apply
request is authenticated and authorized again.

## Dry-run is zero-write

Dry-run opens a read transaction and does not persist a “plan created” event. It leaves unchanged:

```text
Run row and timestamps
Run event count and sequence
checkpoint row, JSON, timestamp, and revision
workflow execution
tool calls
external actions
workflow events
lease fields
queued successor
```

## Atomic apply

Apply must reference the eligible dry-run ID:

```json
{
  "target": {"run_id": "run_123"},
  "resolution": "terminalize_failed_preserving_checkpoint",
  "dry_run": false,
  "expected_plan_id": "qrp_<sha256>"
}
```

The SQLite primitive uses one short `BEGIN IMMEDIATE` transaction across the Run, checkpoint,
workflow, tool, action, and event tables:

1. reread all preconditions;
2. regenerate the current plan;
3. compare the current and expected plan IDs;
4. require the exact lease-free quarantine state;
5. update the Run to `failed` with `thread_checkpoint_conflict`;
6. leave the checkpoint and workflow/tool/action ledgers untouched;
7. append `quarantine.resolution_applied`;
8. append `run.failed`;
9. commit atomically.

Any plan drift returns `409 quarantine_resolution_plan_stale` and writes nothing. Event-append
failure rolls back the Run transition and any earlier event in the transaction. No provider, LLM,
Runtime execution, network call, or wait occurs in the transaction.

The audit event records the resolution kind, plan ID, source quarantine code, base/current
checkpoint revisions, preserved dispositions, provider call count `0`, non-secret operator subject
and credential IDs, sanitized evidence fingerprints, and the typed plan.

## Exact replay and readback verification

The HTTP response can be lost after SQLite commits. Repeating the same target, resolution kind, and
plan ID detects the one committed audit event and returns:

```json
{
  "outcome": "reused",
  "reused": true,
  "verified": true,
  "plan": {"plan_id": "qrp_<same-sha256>"}
}
```

Replay does not append another event or terminalize again. A different plan ID fails closed; merely
finding a terminal Run is never treated as proof of replay.

Before either `applied` or `reused` is returned, the service rereads durable state and requires:

```text
Run.status == failed
Run.error_code == thread_checkpoint_conflict
all lease fields cleared
checkpoint revision and content unchanged
workflow/tool/action evidence fingerprint unchanged
exactly one matching resolution event
exactly one matching run.failed event
```

If readback is incomplete, the API returns
`500 quarantine_resolution_evidence_incomplete`. A fresh commit is not rolled back after this
post-commit observation; the error tells the operator not to assume success and to inspect durable
evidence.

## Incident SOP

1. Stop new submissions for the affected tenant-qualified Thread.
2. Preserve a consistent SQLite backup, including WAL state, and the provider-side evidence.
3. Inspect the Run/Action and durable events without modifying the database.
4. Submit dry-run and confirm `eligible=true`, `provider_calls=0`, checkpoint `preserved`, external
   evidence `preserved`, and the expected terminal status summary.
5. Preserve the returned plan ID in the incident record.
6. Apply that exact plan once. If it is stale, do not retry the old plan; inspect the new evidence
   and run dry-run again.
7. Confirm `verified=true`, one audit event, the failed Run, unchanged checkpoint revision/content,
   unchanged workflow/action evidence, and normal successor claimability.
8. Resume Thread submissions only after the durable readback is understood.

Never edit Run/checkpoint/lease/action rows, delete events, cancel as an unquarantine shortcut,
repeat the operation on a new Thread, or infer a provider outcome.

## Boundary and non-goals

This is a SQLite single-host composite transaction. It is not a PostgreSQL or multi-host proof and
does not provide:

- automatic unquarantine, reconciliation, resume, retry, or redrive;
- checkpoint rebase, rollback, merge, replacement, or projection;
- provider query, dispatch, retry, compensation, or exactly-once effects;
- terminalization of prepared/dispatching/reconciling actions;
- arbitrary repair DSL, SQL, field patching, or an operator UI;
- modification or deletion of workflow, tool, action, checkpoint, or prior event evidence;
- a production incident-management platform.

Executable coverage is in `tests/test_quarantine_resolution.py`,
`tests/test_quarantine_resolution_api.py`, and conformance invariant I9 in
`tests/conformance/test_execution_plane_contract.py`.
