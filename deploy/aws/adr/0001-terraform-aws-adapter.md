# ADR 0001: Terraform for the first AWS deployment adapter

- Status: Accepted
- Date: 2026-08-27
- Scope: P6A infrastructure lifecycle only

## Problem

The Runtime already owns durable execution semantics in PostgreSQL, but the repository cannot yet
recreate, review, operate, or remove a proof deployment. The missing value is not a second scheduler
or an AWS-shaped application API. It is a falsifiable infrastructure lifecycle whose identity,
network, IAM, secret, cost, state, and teardown decisions remain visible in review.

The first provider implementation must also preserve the portable-substrate direction:

```text
AWS deployment adapter -> portable process contract -> PostgreSQL Runtime authority
```

## Existing solutions considered

| Option | Reviewable dry run | State/drift/destroy | IAM/network visibility | Credential-free CI | Fit for this proof |
| --- | --- | --- | --- | --- | --- |
| Terraform + AWS provider | Native plan and explicit resource graph | Explicit state, locking, refresh, and destroy | Direct HCL resources and policy JSON | Provider-schema validation plus mock-provider plans | Best match |
| AWS CDK | `synth`/`diff`, then CloudFormation | CloudFormation stack lifecycle | Constructs can hide generated roles and asset resources | Synthesis is offline, deployment bootstrapping is not | Adds a layer without closing a project gap |
| AWS Copilot | Convenient service-oriented manifests | Managed through generated CloudFormation | Deliberately hides much of VPC/IAM/task detail | Local validation is possible | Hides boundaries this phase must learn and prove |
| Raw CloudFormation | Change sets and stack lifecycle | Native stack state | Direct but verbose | Template validation is weaker without AWS | No concrete advantage over Terraform here |

## Decision

Use Terraform as an AWS-specific adapter under `deploy/aws`. Keep two flat root configurations:

- `bootstrap` owns the private, encrypted, versioned S3 state bucket;
- `proof` owns the proof VPC, ECR, RDS, ECS, IAM, Secrets Manager container, logs, and budget.

The proof root declares an empty S3 backend. Account-specific backend coordinates are supplied only
after `bootstrap` is planned and explicitly approved. S3 native lockfiles are used instead of adding
a DynamoDB table solely for state locking.

CI uses the real locked AWS provider schema with Terraform mock providers. It exercises plan-time
variable validation, lifecycle preconditions, resource composition, and assertions without AWS
credentials or provider API calls. This is stronger than text validation but is not represented as
a live AWS plan.

## Provider-specific design choices

- Two AZs provide a valid RDS subnet group, but one RDS instance with `multi_az=false` keeps the
  proof explicitly non-HA.
- RDS has no public route or address. Only the Runtime security group may reach TCP 5432.
- Runtime tasks have no inbound rule. They use public IPv4 egress with bounded security-group
  ports so the first proof avoids NAT Gateway, interface-endpoint, and ALB fixed charges. Public
  IPv4 remains a per-task hourly cost and is included in the P6C estimate.
- The RDS parameter group forces TLS transport. CA and hostname verification remain a separate
  P6B.2 exact-image gate; `rds.force_ssl=1` alone is not claimed as `verify-full` evidence.
- RDS manages its master password in Secrets Manager. Terraform creates the Runtime DSN secret
  container but never a secret version, because Terraform would otherwise retain plaintext in
  state. A versioned P6C bootstrap action seeds it before any Runtime task may start.
- Application and bootstrap task roles contain no AWS permissions. The ECS execution role may read
  only the Runtime DSN secret required for injection.
- GitHub Actions OIDC trust binds the exact repository, `sts.amazonaws.com` audience, and `proof`
  GitHub Environment. The release role is not an infrastructure-administrator role.
- The AWS Budget is account-wide. A tag-filtered budget can miss spend until a cost-allocation tag
  is activated and propagated; existing account spend therefore consumes the approved ceiling.

## Why this is not a multi-cloud abstraction

No `CloudProvider` interface, provider-neutral Terraform module, or runtime plugin loader is added.
Terraform resources are allowed to use AWS capabilities directly. A future provider receives a
sibling adapter and must satisfy the same portable process and operational evidence contract. Shared
infrastructure modules are extracted only after a second real implementation reveals stable
duplication.

## Consequences and follow-up gates

- P6A can be reviewed and merged without cloud credentials or cloud mutation.
- A mock plan proves configuration behavior, not service availability, regional engine support,
  IAM acceptance, price, or destroy behavior. P6C must obtain a real saved plan and authoritative
  AWS rereads.
- The no-NAT/no-ALB topology has no public application ingress. Health, readiness, bootstrap, and
  semantic smoke run as ECS-local proof actions until a later workload demonstrates an ingress need.
- The bootstrap state bucket intentionally refuses recursive deletion. It is removed only after
  the proof stack, evidence, state versions, and retention choices are reconciled.
- P6B.2 still owns exact-image PostgreSQL CA/hostname verification, independent container lifecycle,
  JSON-log canaries, semantic smoke, and bounded SIGTERM evidence.
