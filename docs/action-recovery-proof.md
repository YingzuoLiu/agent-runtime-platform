# Durable Action recovery proof

This local proof makes the Phase 7D recovery boundary visible without replacing it with a mock
workflow. It uses the published `POST /actions` API, the real `HttpExternalActionProvider`, the real
external-action ledger and lease-aware `RuntimeManager` recovery path, and a separate provider
process with its own SQLite effect ledger.

## Run it

From the repository root with Docker Desktop running:

```powershell
python examples/action_recovery_proof.py
```

The first run builds the image. Run that once before an interview. A warmed repeat can skip the
build step:

```powershell
python examples/action_recovery_proof.py --no-build
```

The script starts the two Compose services, submits each Action, waits until the provider proves the
effect is committed, sends `SIGKILL` to the exact Compose `runtime` service, releases an injected
provider-side `503`, and restarts the Runtime over its existing volume. Before restart, a local-only
probe extends the killed attempt's still-live lease to create a deterministic observation window.
The restarted Manager must leave that attempt untouched. The probe then moves that exact attempt to
the SQLite store-time expiry boundary, after which the Manager must recover it exactly once. The
script never removes volumes or resets repository state. If any assertion fails, the command exits
nonzero.

A passing summary is:

```text
ACTION GATEWAY RECOVERY PROOF: PASSED
  safe-retry:       2 attempts, 1 effect, succeeded with stored receipt
  unsafe-no-retry:  1 attempt, 1 effect, outcome_unknown without replay
  each live lease resisted takeover, then recovered once at exact expiry
  Runtime restarted twice; provider lifecycle stayed unchanged
  Sanitized artifact: .../artifacts/action-recovery-proof.json
```

The generated artifact is ignored by Git. It excludes the demo API key, client/provider
idempotency keys, request body, tenant and subject, endpoint configuration, credentials, dispatch
tokens, Run lease tokens and owners, lease timestamps, and raw upstream responses. Its Run-lease
section contains only attempt numbers, event counts, the public recovery reason, and proof booleans.
The script removes the previous generated artifact before starting, so a failed rerun cannot leave a
stale PASS artifact behind.

## What the two paths prove

- `safe-retry` declares `supports_idempotency=true`. After restart, the Runtime finds the provider
  effect committed and its own action still `dispatching`. It dispatches the same server-derived
  key once more, receives the stored receipt, and finishes `succeeded`.
- `unsafe-no-retry` declares `supports_idempotency=false`. From the same persisted state, the
  Runtime makes no second provider call and finishes `outcome_unknown`.

Both rows must finish with `effect_count=1`. The second row is not a degraded success path: the
Runtime deliberately reports uncertainty rather than risking a duplicate real-world write.

Submitting the same public Action again must also return the original `action_id` and terminal
status without another provider attempt.

For both paths, the enclosing private Run must remain at attempt 1 while the killed attempt's lease
is live. At exact expiry it must advance to attempt 2 with one additional `run.started` event and
exactly one `run.recovered` event whose reason is `lease_expired`. This is the cross-milestone
property: Run takeover and external-action reconciliation must remain correct when exercised
together.

## Failure injection and evidence order

The sidecar does not merely echo the request. On the first dispatch it:

1. validates the `ExternalActionRequest` and matching `Idempotency-Key` header;
2. writes the effect and sanitized provider events to its own SQLite ledger;
3. holds the HTTP response so the proof script can observe `effect.committed`;
4. after the Runtime is killed, records `fault.release_requested` and `response.ambiguous`, then
   returns `503` to the now-lost connection;
5. while that attempt still has a live lease, observes that the restarted Runtime neither advances
   the Run attempt nor calls the provider again;
6. injects exact store-time expiry and waits for durable `run.recovered(reason=lease_expired)` plus
   the second `run.started` event before checking the Action outcome.

Provider event order comes from its monotonically increasing `event_sequence`; the proof never
compares clocks across the Runtime and sidecar. Runtime Action events use the existing durable
workflow sequence. Run ownership evidence comes from a strict allowlist in
`examples/runtime_lease_probe.py`; its SQL never selects the lease token or owner. The probe uses
SQLite's same `julianday('now')` expression and `BEGIN IMMEDIATE` transaction boundary as the Run
store.

For `safe-retry`, the post-restart request finds the existing hashed key and canonical request hash,
adds `receipt.replayed`, and returns `200 application/json` with the strict provider result:

```json
{
  "provider_reference": "delivery_<opaque>",
  "result": {}
}
```

For `unsafe-no-retry`, a second provider request would create another effect. The proof therefore
fails if `attempt_count` or `effect_count` exceeds one; the expected Runtime behavior avoids that
call entirely.

## Why the services share a network namespace

The HTTP adapter requires HTTPS except for an explicitly enabled loopback development endpoint.
Compose uses:

```yaml
network_mode: service:demo-provider
```

This makes `http://127.0.0.1:8100` refer to the provider from inside the Runtime while preserving
the adapter's `allow_insecure_localhost=true` restriction. It is a deliberate local-proof topology,
not a production service-discovery recommendation. The provider owns the shared namespace and
publishes both loopback ports; the Runtime declares no port mapping of its own.

The proof also asserts that the provider container's `StartedAt` value stays unchanged while the
Runtime's value changes on each restart. The separate `provider-data` and `runtime-data` volumes
preserve the two ledgers independently.

## Two-minute interview walkthrough

Start with the user-facing reason rather than the JSON:

> An Agent can make the right decision, but a real action may finish just as the Runtime crashes.
> The hard question is whether retrying is safe. This proof makes the Runtime handle both answers
> explicitly instead of guessing.

Then run the warmed command and point to the two PASS lines:

1. **Safe retry:** “The provider had already committed. After restart, the Runtime reused the same
   server-derived key, recovered the stored receipt, and still produced only one effect.”
2. **Unsafe retry:** “This destination does not promise idempotency, so the Runtime did not
   resend. It reported `outcome_unknown`; that is safer than silently duplicating the action.”
3. **Fenced takeover:** “The restarted Manager first left the live attempt alone. At exact lease
   expiry, attempt 2 took over with one `lease_expired` recovery event.”
4. **Durability:** “The Runtime process restarted twice, while the provider process and both
   SQLite ledgers survived. Repeating the public request reused the same Action.”

If the interviewer wants implementation detail, open the sanitized artifact and show only:

- final Action status;
- Runtime event `dispatch_count` and `retry_mode`;
- provider `attempt_count=1|2` and `effect_count=1`;
- Run attempt `1 -> 2`, one recovery event, and `reason=lease_expired`;
- ordered provider events ending in `receipt.replayed` only for the idempotent path.

The Travel Runtime Console remains the first, product-facing demonstration. This recovery proof is
the second act for an interviewer who asks what the platform contributes beyond planning and tool
calling.

## Boundaries

This is a deterministic local proof, not a production provider. Its control endpoints are
unauthenticated and published only on `127.0.0.1`; the Compose file is marked local-demo-only. The
lease probe is a test-only direct-database fault-injection seam, not a Runtime API or an operational
lease-management tool. Lease-deadline and exact-expiry fencing semantics remain covered by the Run
leasing test suite; this proof covers the real Docker restart wiring and its interaction with
Action reconciliation. Phase 7D still does not claim distributed exactly-once execution,
compensation, provider-status queries, automatic terminal-unknown resolution, arbitrary webhooks,
or production sidecar deployment.
