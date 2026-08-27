variable "aws_account_id" {
  description = "Exact 12-digit AWS account selected at the P6C authorization gate."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be an exact 12-digit AWS account ID."
  }
}

variable "aws_partition" {
  description = "AWS ARN partition. The first proof environment uses the commercial aws partition."
  type        = string
  default     = "aws"

  validation {
    condition     = contains(["aws", "aws-us-gov", "aws-cn"], var.aws_partition)
    error_message = "aws_partition must be aws, aws-us-gov, or aws-cn."
  }
}

variable "aws_region" {
  description = "Exact AWS region selected at the P6C authorization gate."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "aws_region must look like us-east-1."
  }
}

variable "availability_zones" {
  description = "Two explicit AZs. RDS is single instance/non-HA, but its subnet group spans two AZs."
  type        = list(string)

  validation {
    condition     = length(var.availability_zones) == 2 && length(distinct(var.availability_zones)) == 2
    error_message = "availability_zones must contain exactly two distinct zones."
  }
}

variable "project_name" {
  description = "Stable project identifier used in names and ownership tags."
  type        = string
  default     = "agent-runtime-platform"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,31}$", var.project_name))
    error_message = "project_name must be 3-32 lowercase letters, digits, or hyphens."
  }
}

variable "environment" {
  description = "Proof environment identifier."
  type        = string
  default     = "proof"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be 2-16 lowercase letters, digits, or hyphens."
  }
}

variable "owner" {
  description = "Human owner recorded on cost and lifecycle tags."
  type        = string

  validation {
    condition     = length(trimspace(var.owner)) >= 2
    error_message = "owner must identify the human responsible for this proof environment."
  }
}

variable "vpc_cidr" {
  description = "CIDR for the proof VPC."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR."
  }
}

variable "runtime_subnet_cidrs" {
  description = "Public-routed ECS subnets. Tasks receive public egress addresses but no inbound security-group rule."
  type        = list(string)
  default     = ["10.42.0.0/24", "10.42.1.0/24"]

  validation {
    condition     = length(var.runtime_subnet_cidrs) == 2 && alltrue([for cidr in var.runtime_subnet_cidrs : can(cidrnetmask(cidr))])
    error_message = "runtime_subnet_cidrs must contain exactly two IPv4 CIDRs."
  }
}

variable "database_subnet_cidrs" {
  description = "Isolated RDS subnets with no internet route."
  type        = list(string)
  default     = ["10.42.10.0/24", "10.42.11.0/24"]

  validation {
    condition     = length(var.database_subnet_cidrs) == 2 && alltrue([for cidr in var.database_subnet_cidrs : can(cidrnetmask(cidr))])
    error_message = "database_subnet_cidrs must contain exactly two IPv4 CIDRs."
  }
}

variable "source_revision" {
  description = "Accepted lowercase Git commit embedded in runtime readiness identity."
  type        = string

  validation {
    condition     = can(regex("^([0-9a-f]{40}|[0-9a-f]{64})$", var.source_revision))
    error_message = "source_revision must be a 40- or 64-character lowercase hexadecimal commit."
  }
}

variable "image_digest" {
  description = "Immutable OCI digest selected after the exact image build and registry reread."
  type        = string

  validation {
    condition     = can(regex("^sha256:[0-9a-f]{64}$", var.image_digest))
    error_message = "image_digest must use sha256:<64 lowercase hex characters>."
  }
}

variable "postgres_engine_version" {
  description = "Exact PostgreSQL engine version verified as available in the selected region before P6C plan."
  type        = string

  validation {
    condition     = can(regex("^16\\.[0-9]+$", var.postgres_engine_version))
    error_message = "postgres_engine_version must pin an exact PostgreSQL 16 minor version."
  }
}

variable "db_instance_class" {
  description = "Small proof-only RDS instance class."
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage_gib" {
  description = "Initial encrypted gp3 storage allocation."
  type        = number
  default     = 20

  validation {
    condition     = var.db_allocated_storage_gib >= 20 && var.db_allocated_storage_gib <= 100
    error_message = "db_allocated_storage_gib must remain between 20 and 100 GiB for the proof environment."
  }
}

variable "db_backup_retention_days" {
  description = "Bounded automated backup retention."
  type        = number
  default     = 1

  validation {
    condition     = var.db_backup_retention_days >= 0 && var.db_backup_retention_days <= 7
    error_message = "db_backup_retention_days must remain between 0 and 7."
  }
}

variable "rds_deletion_protection" {
  description = "Whether RDS deletion protection is enabled for the approved exercise."
  type        = bool
  default     = false
}

variable "skip_final_snapshot" {
  description = "Explicit P6C retention choice; no default is allowed."
  type        = bool
}

variable "final_snapshot_identifier" {
  description = "Explicit unique RDS final-snapshot name when skip_final_snapshot is false."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = (
      var.final_snapshot_identifier == null ||
      can(regex("^[a-z][a-z0-9-]{2,62}$", var.final_snapshot_identifier))
    )
    error_message = "final_snapshot_identifier must be null or a 3-63 character lowercase RDS identifier."
  }
}

variable "runtime_secret_recovery_window_days" {
  description = "Explicit Secrets Manager deletion recovery window; use 0 only when immediate proof teardown is approved."
  type        = number

  validation {
    condition = (
      var.runtime_secret_recovery_window_days == 0 ||
      (var.runtime_secret_recovery_window_days >= 7 && var.runtime_secret_recovery_window_days <= 30)
    )
    error_message = "runtime_secret_recovery_window_days must be 0 or between 7 and 30."
  }
}

variable "runtime_secret_is_seeded" {
  description = "Authoritative operator assertion set only after the DSN secret has a current value and redaction checks pass."
  type        = bool
  default     = false
}

variable "service_desired_count" {
  description = "Number of independent Runtime tasks. Zero is the safe pre-secret/pre-bootstrap default."
  type        = number
  default     = 0

  validation {
    condition     = var.service_desired_count >= 0 && var.service_desired_count <= 2 && floor(var.service_desired_count) == var.service_desired_count
    error_message = "service_desired_count must be an integer from 0 to 2."
  }
}

variable "runtime_worker_count" {
  description = "RuntimeManager workers per container. P6 keeps one per independently scheduled task."
  type        = number
  default     = 1

  validation {
    condition     = var.runtime_worker_count == 1
    error_message = "P6 requires exactly one RuntimeManager worker per ECS task."
  }
}

variable "runtime_http_concurrency_limit" {
  description = "Bounded Uvicorn HTTP admission limit."
  type        = number
  default     = 32

  validation {
    condition     = var.runtime_http_concurrency_limit >= 1 && var.runtime_http_concurrency_limit <= 256
    error_message = "runtime_http_concurrency_limit must be between 1 and 256."
  }
}

variable "manager_shutdown_grace_seconds" {
  description = "RuntimeManager drain budget."
  type        = number
  default     = 5
}

variable "server_shutdown_grace_seconds" {
  description = "ASGI graceful shutdown budget."
  type        = number
  default     = 15
}

variable "ecs_stop_timeout_seconds" {
  description = "ECS SIGTERM-to-SIGKILL timeout; must exceed the ASGI budget."
  type        = number
  default     = 30

  validation {
    condition     = var.ecs_stop_timeout_seconds >= 1 && var.ecs_stop_timeout_seconds <= 120
    error_message = "ecs_stop_timeout_seconds must be between 1 and 120."
  }
}

variable "fargate_platform_version" {
  description = "Explicit Linux Fargate platform selected for evidence."
  type        = string
  default     = "1.4.0"
}

variable "log_retention_days" {
  description = "CloudWatch retention for proof logs."
  type        = number
  default     = 7

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30], var.log_retention_days)
    error_message = "log_retention_days must be a supported bounded proof value."
  }
}

variable "monthly_budget_usd" {
  description = "Maximum monthly proof budget configured in AWS Budgets."
  type        = number

  validation {
    condition     = var.monthly_budget_usd >= 5 && var.monthly_budget_usd <= 500
    error_message = "monthly_budget_usd must be between 5 and 500."
  }
}

variable "budget_alert_email" {
  description = "Operator email that must confirm AWS Budget notifications before P6C acceptance."
  type        = string

  validation {
    condition     = can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.budget_alert_email))
    error_message = "budget_alert_email must be a plausible email address."
  }
}

variable "github_repository" {
  description = "Exact GitHub owner/repository allowed to request the release role."
  type        = string
  default     = "YingzuoLiu/agent-runtime-platform"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must use owner/repository form."
  }
}

variable "github_environment" {
  description = "GitHub Environment encoded into the OIDC subject claim."
  type        = string
  default     = "proof"

  validation {
    condition     = length(trimspace(var.github_environment)) >= 2
    error_message = "github_environment must name the approval-gated GitHub Environment."
  }
}

variable "existing_github_oidc_provider_arn" {
  description = "Existing account-level GitHub OIDC provider ARN, or null to let this stack create it."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = (
      var.existing_github_oidc_provider_arn == null ||
      can(regex("^arn:[^:]+:iam::[0-9]{12}:oidc-provider/token\\.actions\\.githubusercontent\\.com$", var.existing_github_oidc_provider_arn))
    )
    error_message = "existing_github_oidc_provider_arn must be the account GitHub Actions OIDC provider ARN."
  }
}
