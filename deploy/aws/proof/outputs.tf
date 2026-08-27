output "release_identity" {
  description = "Immutable application identity supplied to every task."
  value = {
    source_revision = var.source_revision
    image_digest    = var.image_digest
    image           = local.runtime_image
  }
}

output "runtime_secret_arn" {
  description = "Secret container ARN only; the DSN value is never represented by Terraform."
  value       = aws_secretsmanager_secret.runtime_postgres_dsn.arn
}

output "rds_master_secret_arn" {
  description = "RDS-managed master secret ARN; its value is not exposed."
  value       = try(aws_db_instance.runtime.master_user_secret[0].secret_arn, null)
}

output "rds_endpoint" {
  description = "Private PostgreSQL endpoint used by the P6C secret-bootstrap step."
  value       = aws_db_instance.runtime.endpoint
}

output "ecs_release" {
  description = "Bounded release surfaces used by the future GitHub OIDC workflow."
  value = {
    cluster_arn                   = aws_ecs_cluster.runtime.arn
    service_arn                   = aws_ecs_service.runtime.id
    runtime_task_definition_arn   = aws_ecs_task_definition.runtime.arn
    bootstrap_task_definition_arn = aws_ecs_task_definition.bootstrap.arn
    github_release_role_arn       = aws_iam_role.github_release.arn
  }
}

output "network_posture" {
  description = "Reviewable proof topology; this is explicitly not a high-availability claim."
  value = {
    high_availability       = false
    load_balancer           = false
    nat_gateway             = false
    runtime_public_egress   = true
    runtime_inbound_rules   = 0
    rds_publicly_accessible = aws_db_instance.runtime.publicly_accessible
    rds_multi_az            = aws_db_instance.runtime.multi_az
  }
}

output "p6c_authorization_gate" {
  description = "Non-secret consequential inputs that must exactly match the user's approval before any apply."
  value = {
    aws_account_id                      = var.aws_account_id
    aws_region                          = var.aws_region
    environment                         = var.environment
    availability_zones                  = var.availability_zones
    monthly_budget_usd                  = var.monthly_budget_usd
    service_desired_count               = var.service_desired_count
    skip_final_snapshot                 = var.skip_final_snapshot
    rds_deletion_protection             = var.rds_deletion_protection
    runtime_secret_recovery_window_days = var.runtime_secret_recovery_window_days
  }
}
