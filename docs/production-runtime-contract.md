# Portable production Runtime contract

## Problem, existing mechanism, and gap

The application already has one coherent SQLite or PostgreSQL durable authority, explicit
PostgreSQL bootstrap, read-only startup validation, `tini` as PID 1, and public liveness/readiness
probes. The local Compose demo also already supplies its SQLite path and synthetic provider
explicitly.

The previous image was not yet a production-shaped PostgreSQL artifact: it installed only the
base requirements, set an image-level SQLite path, used Uvicorn's text logging defaults, exposed no
release identity, and placed no upper bound on HTTP admission. PostgreSQL startup also required a
real or HTTP Travel Action provider even when a proof environment was intentionally forbidden from
making external writes.

This contract closes those process and image gaps without creating a cloud-provider abstraction.
The same image remains runnable on any substrate that supplies the portable substrate contract.

## Deterministic process boundary

The production image starts exactly one ASGI process:

```text
tini (PID 1)
  -> python -m runtime_service.serve
       -> one Uvicorn process
            -> one bounded RuntimeManager worker pool
```

Multiple independently scheduled containers supply horizontal concurrency. Uvicorn process
workers are deliberately fixed at one so process multiplication cannot silently multiply the
RuntimeManager pool or database-session budget. `RUNTIME_WORKER_COUNT` is limited to `1..16`; the
P6 proof topology uses one Runtime worker per container.

The portable entrypoint also enforces:

| Input | Default | Accepted boundary |
| --- | ---: | --- |
| `RUNTIME_WORKER_COUNT` | `1` | integer `1..16` |
| `RUNTIME_HTTP_CONCURRENCY_LIMIT` | `32` | integer `1..256` |
| `RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS` | `5` | `0..120` seconds |
| `RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS` | `15` | integer `1..120`, strictly greater than manager grace |
| `RUNTIME_LOG_LEVEL` | `info` | `debug`, `info`, `warning`, `error`, or `critical` |
| `RUNTIME_EXTERNAL_ACTION_MODE` | `enabled` | `enabled` or `disabled` |

Validate this process configuration without binding a port or starting application work:

```bash
python -m runtime_service.serve --check
```

The command emits one credential-clean JSON object containing only these bounded values and the
optional release identity. PostgreSQL catalog inspection remains the separate, read-only
`python -m runtime_service.postgres_bootstrap --dry-run` authority.
Invalid process configuration emits one value-free JSON failure on stderr and exits `2` before
binding a port or importing the application.

The eventual ECS `stopTimeout` must exceed the configured server graceful-shutdown budget. P6 uses
an explicit `SIGTERM` stop signal; `tini` forwards it, Uvicorn stops HTTP admission, and the
application lifespan calls `RuntimeManager.stop()`. The Manager stops claiming first, waits within
its own shorter grace, and disables renewal before returning if a synchronous Runtime call cannot
finish. PostgreSQL lease expiry and fencing remain the final authority.

Current Uvicorn restores and re-raises the captured `SIGTERM` after the graceful shutdown sequence,
so a container runtime may record exit code `143`. The deployment proof must correlate that code
with an explicit ECS stop reason and the complete application-shutdown log sequence; it must not
rewrite every signal exit to zero or treat an unexplained `143` as success.

## External-write-disabled proof mode

`RUNTIME_EXTERNAL_ACTION_MODE=disabled` rejects every injected or environment-configured Action
provider and installs an explicit
`DisabledExternalActionProvider`. It has a stable, non-secret identity, advertises no idempotency
capability, so provider-idempotent tools fail deterministically at policy preflight without
creating an Action row or entering provider code. If a non-idempotent tool reaches the adapter, it
returns a definitive failure. The mode cannot be combined with
`RUNTIME_ACTION_PROVIDERS_JSON`, an injected provider, or any
`RUNTIME_TRAVEL_ACTION_PROVIDER_*` configuration.

This is not a mock success. Read-only and deterministic semantic probes can use the normal Runtime,
while any request that asks for a real Travel external write fails closed. P7, not P6, owns the
first real GitHub effect.

## Release identity

Deployment may supply an auditable pair:

```text
RUNTIME_SOURCE_REVISION=<40- or 64-character lowercase Git commit>
RUNTIME_IMAGE_DIGEST=sha256:<64 lowercase hex characters>
```

The values must be present together. Setting `RUNTIME_RELEASE_IDENTITY_REQUIRED=true` makes their
absence a startup error. When present, `/ready` reports them alongside the already-safe storage
metadata. The application does not query ECS/EC2 metadata and does not declare the supplied digest
authoritative by itself: deployment evidence must compare it with the registry digest and task
definition after deployment.

## Logs and secrets

The entrypoint configures Uvicorn application, error, and access output as one-line JSON on stdout.
Fields are bounded to timestamp, level, logger, message, optional correlation identifiers, and a
locals-free exception rendering. Configured DSNs, API-key documents, provider documents, extracted
passwords, API keys, and bearer tokens are redacted if a dependency includes them in a log message.

Redaction is a backstop, not permission to log secrets. Existing configuration and provider errors
remain deliberately value-free. The substrate transports JSON; no CloudWatch-only envelope enters
the application contract.

## Image behavior

The production image:

- installs both base and PostgreSQL requirements;
- selects `RUNTIME_STORE_BACKEND=postgres` and has no image-level `RUNTIME_DB_PATH`, so a missing
  PostgreSQL DSN fails startup instead of silently creating container-local SQLite authority;
- runs as UID `10001` behind `tini`;
- declares `SIGTERM` explicitly;
- uses `/health` for container liveness while deployment and semantic gates call `/ready`;
- starts through `python -m runtime_service.serve`.

The distinction is deliberate: a database outage should make readiness fail and block acceptance,
but a generic image liveness probe should not create an ECS replacement loop that cannot repair the
database. ECS deployment health alone remains insufficient; the release gate must call `/ready` and
then complete the authoritative PostgreSQL smoke transition.

The local Compose demo remains unchanged at the product boundary: it explicitly overrides
`RUNTIME_STORE_BACKEND=sqlite` and supplies `RUNTIME_DB_PATH=/app/runtime_data/runtime.db` plus
`RUNTIME_DEMO_MODE=true` itself. Those values are not inherited by a proof deployment.

## Remaining P6B work

This slice does not claim the complete P6 deployment gate. The next portable-image proof must still
exercise the exact image against PostgreSQL with CA/hostname verification, explicit bootstrap,
two independent containers, a durable semantic transition, JSON-log secret canaries, and bounded
`SIGTERM` shutdown. Optional W3C/OTLP telemetry is also still required before real P7 traffic.

Heartbeat renewal risk R14 remains unchanged. A transient renewal exception is fail-safe but can
abandon an attempt until its lease expires. Any retry policy needs a dedicated remaining-window
scenario and mutation control before production semantics change.
