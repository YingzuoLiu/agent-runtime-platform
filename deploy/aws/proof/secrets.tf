resource "aws_secretsmanager_secret" "runtime_postgres_dsn" {
  name        = "${local.name}/runtime-postgres-dsn"
  description = "Seeded outside Terraform state by the deterministic P6C secret-bootstrap step"

  recovery_window_in_days = var.runtime_secret_recovery_window_days
}

# Secret values are deliberately outside Terraform authority. Supplying a DSN
# through Terraform would persist the plaintext in state.
