# Portable Substrate Contract

## Problem

The runtime needs a reproducible AWS proof environment without allowing AWS service semantics to
become execution authority. Running the same container on AWS is not itself vendor lock-in. The
lock-in boundary is crossed when a cloud queue, workflow service, task identity, proprietary log
format, or provider-only authentication path becomes necessary for Run correctness.

This contract keeps the durable runtime portable while allowing the deployment layer to use
provider-specific infrastructure deliberately.

## Existing boundary

The accepted runtime already owns its durable semantics:

- PostgreSQL or SQLite implements Run, Workflow, Memory, checkpoint, lease, and fencing state;
- PostgreSQL server time is the production lease-time authority;
- bounded database polling, not a process-local wake signal, provides cross-process progress;
- `RuntimeManager` creates an opaque owner identity without consulting host or task metadata;
- external effects enter through an injected provider boundary over ordinary HTTPS;
- the process receives configuration through environment variables and exposes HTTP health and
  readiness endpoints.

The application packages do not import a cloud SDK. PostgreSQL support is an explicit optional
runtime dependency rather than a cloud database API.

## Value gap

Those properties were implicit. A future change could still introduce a cloud SDK, read task
metadata as execution identity, replace database fencing with queue visibility, or emit a
provider-only log envelope without an obvious semantic test failing.

The repository therefore treats portability as an executable dependency and authority boundary,
not as a claim that every cloud has been certified.

## Dependency direction

```text
provider deployment adapter -> portable application contract -> runtime authority
```

Provider deployment code may configure and run the application. Application packages must not
import provider deployment code or require a provider SDK. No speculative `CloudProvider`
abstraction is introduced: the boundary consists of standard protocols and observable behavior.

The portable application surface is currently:

```text
agent/
api/
domains/
runtime_service/
```

AWS-specific Terraform, policies, task definitions, and evidence collectors belong outside that
surface under the deployment boundary.

## Required substrate capabilities

| Capability | Portable contract | Deterministic owner |
| --- | --- | --- |
| Durable storage | PostgreSQL DSN, schema, standard PostgreSQL TLS, finite timeouts | Runtime stores and PostgreSQL |
| Configuration | Ordinary environment variables or mounted files | Application configuration validation |
| Process lifecycle | PID 1 signal forwarding and `SIGTERM` grace | Container entrypoint and `RuntimeManager` |
| Networking | Explicit outbound HTTPS and PostgreSQL reachability | Deployment network policy |
| Health | Read-only HTTP `/health` and `/ready` | Application |
| Logs | Structured JSON on stdout/stderr | Application; substrate only transports it |
| Traces | W3C trace context and OTLP export | Application instrumentation |
| Release identity | OCI digest plus source revision supplied as non-secret configuration | Build and deployment evidence |

A deployment adapter may obtain values from Secrets Manager, another secret manager, or a local
file, but the application receives the same secret value through the portable configuration
boundary. Secret-provider lookup is not execution authority.

## Forbidden coupling inside the portable surface

The portable application must not:

- import AWS, Google Cloud, Azure, Kubernetes, or another substrate-management SDK;
- import from repository deployment modules;
- derive worker identity or lease ownership from ECS, EC2, or another metadata endpoint;
- use SQS receipt/visibility semantics, Step Functions state, or DynamoDB state as the authority
  for Run leasing, fencing, checkpointing, or recovery;
- require ARN/account/region values for core Run correctness;
- make RDS IAM authentication the only supported PostgreSQL authentication path;
- emit CloudWatch Embedded Metric Format as the only structured-log contract.

An integration adapter outside the portable surface may use a provider SDK when a concrete feature
requires it. Its result must still enter the runtime through an existing typed boundary; it cannot
replace the runtime's authority model.

## Executable enforcement

`scripts/check_substrate_contract.py` deterministically scans the portable source imports,
executable string constants, and production dependency manifests. It rejects known cloud SDKs,
reverse imports from deployment code, and provider-specific authority markers. Unit tests prove
the scanner rejects representative static, dynamic-import, metadata, queue-authority, and
provider-only authentication examples.

This static tripwire is necessary but not sufficient. P6 application hardening must also run the
exact production OCI image against ordinary PostgreSQL without cloud credentials or metadata and
prove bootstrap, readiness, a durable semantic transition, JSON logs, and `SIGTERM` behavior. AWS
acceptance must reuse that exact image digest.

## What this does not claim

- It does not certify Cloud Run, Azure Container Apps, Kubernetes, Fly.io, or bare-metal operation.
- It does not make Terraform resources or state portable across providers.
- It does not remove PostgreSQL data gravity, version, extension, backup, TLS, or connection-limit
  migration work.
- It does not prohibit AWS-specific infrastructure inside the AWS deployment adapter.

Portability means a substrate replacement does not require changing the accepted runtime authority
or application protocol. Each additional substrate still needs its own deployment adapter and
acceptance evidence.

## Change rule

Any change that weakens this contract must identify the new authority, explain the portability and
recovery consequences, add deterministic evidence for the replacement, and receive an explicit
architecture decision before merge. Disabling or excluding the guard is not an acceptable way to
introduce a provider dependency.
