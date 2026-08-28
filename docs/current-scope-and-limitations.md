# Current scope and limitations

This document is the detailed boundary behind the concise claims in the root README. Its capability
claims are anchored to the accepted P6B.2 baseline (`75740b4`) and must not be read as evidence that
P6C live AWS work has run.

## Operational milestone map

The earlier `Phase 1`-`Phase 7D` names describe product capabilities. The later `P` names describe
the operational-lifecycle program:

| Milestone | Operational outcome |
| --- | --- |
| P4 | PostgreSQL semantic portability and coherent application authority |
| P5 | Independent-worker concurrency, recovery, fencing, and mutation evidence |
| P6 | Portable production runtime plus an authorization-gated AWS proof lifecycle |
| P7 | One real, allowlisted, idempotent GitHub integration through the deployed service |
| P8 | Workload-grounded observability, SLI, proposed SLO, alerts, and runbook |
| P9 | Immutable release, compatible migration, rollback, and forward-fix evidence |
| P10 | Bounded load and failure-injection evidence |
| P11 | Reproducible end-to-end operational evidence bundle and system-property scan |

Only P4, P5, and the offline/portable parts of P6 through P6B.2 are accepted. P6C and P7-P11 are
future gates, not implemented claims.

## Evidence ladder

| Level | What is proved | What is not proved |
| --- | --- | --- |
| Local demo | Docker Compose builds and runs a loopback-only SQLite demo with scripted planning | Internet exposure, multi-process SQLite, production credentials, or customer traffic |
| Portable substrate | One PostgreSQL-authoritative image contract, coherent configuration, health/readiness, JSON logs, release identity, and bounded shutdown | A provider deployment, autoscaling policy, load envelope, HA, or rollback orchestration |
| P5 | Two independently killable workers, shared PostgreSQL authority, lease expiry, fencing, thread order, Action recovery, and 9/9 representative mutants killed | A deployed multi-host service, a distributed queue, multi-AZ failure, or sustained-load capacity |
| P6B.2 | The exact digest-identified production image runs against PostgreSQL `verify-full`, rejects a wrong host, excludes secret canaries from logs, and shuts down within the bound | Live registry push, ECS task execution, live RDS, live IAM/OIDC, or live teardown |
| AWS adapter | Terraform format/validation and credential-free mock-provider plans for bootstrap and proof stacks | A live `plan`, `apply`, evidence collection, rollback, or `destroy` |

P6C is authorization-gated because it creates real cloud resources and possible cost. It begins only
after explicit approval and ends with validated teardown evidence; until then, the repository makes
no live-AWS claim.

## Runtime and storage

- Local Compose intentionally overrides the production default and uses SQLite. PostgreSQL is the
  production authority and requires an explicit schema bootstrap; there is no SQLite-to-PostgreSQL
  data migration.
- PostgreSQL uses bounded short-lived per-operation connections. There is no pool-sizing,
  saturation, throughput, latency, or SLO claim without deployment-level measurement.
- Polling and leases provide durable ownership. The local wake signal is only an optimization, not
  a distributed queue or cross-host notification mechanism.
- The first lease-aware and thread-serialization migrations require the documented drain and
  stop-old-runtime boundaries. Mixed old/new binaries and direct rollback are unsupported.
- P5 is strong process-level evidence, but it is not certification of horizontal scaling,
  multi-host operation, high availability, disaster recovery, or multi-region behavior.

## External effects

- The public Action façade supports only `webhook.send` to server-registered destinations. It is
  not an arbitrary URL, method, header, or credential forwarder.
- Prepare-before-dispatch durability and provider idempotency permit bounded safe recovery. They do
  not create exactly-once delivery.
- When the provider may have committed but replay is unsafe, the durable terminal state is
  `outcome_unknown`. There is no automated reconciliation, compensation, rollback workflow, or
  human-approval system.
- Provider-specific reconciliation remains provider-owned; the repository's providers are local
  deterministic test doubles rather than validated production integrations.

## Security and isolation

- Authentication uses static configured API keys with two roles, Viewer and Operator. There is no
  key rotation service, quota system, token accounting, custom role model, or per-Agent/per-tool
  grant administration.
- Registered tools run with policy checks and subprocess containment. This is not a container,
  gVisor, microVM, or arbitrary untrusted-code sandbox.
- Trusted domain extensions are explicit deployment-time composition. There is no discovery, hot
  loading, package marketplace, or untrusted MCP/plugin installation lifecycle.
- Typed decisions, allowlists, schemas, and policy checks reduce unsafe execution; they are not a
  general prompt-injection detector.

## Product and data surface

- Travel, release-validation, incident-triage, and provider data are synthetic. There is no live
  flight, hotel, booking, payment, inventory, or official vendor-sandbox integration.
- The Runtime Console is a local evidence and interview surface, not an account, tenant, Agent,
  credential, deployment, or Memory administration product.
- Scheduling is serial within the current DAG/dynamic-tool paths. Bounded parallel reads,
  multi-Agent delegation, model fallback, and semantic Memory retrieval are future slices rather
  than current claims.
- Governed Memory stores allowlisted explicit preferences; it has no embeddings or inferred-fact
  retrieval, and forgetting does not erase immutable historical Run evidence.
- There is no production OpenTelemetry backend, evaluation dashboard, alert policy, or validated
  service-level objective.

## Deployment support

- Docker Compose is a supported local demonstration path, not a production composition.
- The OCI image and portable substrate contracts are provider-neutral and are current.
- The AWS adapter is currently offline-validated only. Live P6C proof requires separate explicit
  authorization.
- Kubernetes is not a certified target in the current operational program. The former manifest was
  removed because it conflicted with the PostgreSQL-authoritative image and lacked an exact-image
  lifecycle proof. A future Kubernetes claim would require a maintained adapter, secrets and
  bootstrap contract, provider/version pinning, failure tests, exact-image validation, and teardown
  evidence comparable to the selected AWS path.
- No historical tags are being reconstructed. A new Release should mark either completed P6C
  evidence or an explicit decision that P6B.2 is the final operational boundary.

## Research archive

The active deterministic evaluation harness is [`../eval/`](../eval/). [`../rl/`](../rl/) contains
historical optional experiments whose training dependencies are not installed and whose scripts are
not run by CI. The repository makes no supported training or reproducibility claim for that archive.
