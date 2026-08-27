variable "aws_account_id" {
  description = "Exact 12-digit AWS account selected at the P6C authorization gate."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be an exact 12-digit AWS account ID."
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
