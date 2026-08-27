resource "aws_db_parameter_group" "runtime" {
  name   = local.name
  family = "postgres16"

  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_instance" "runtime" {
  identifier = local.name

  engine         = "postgres"
  engine_version = var.postgres_engine_version
  instance_class = var.db_instance_class

  allocated_storage = var.db_allocated_storage_gib
  storage_type      = "gp3"
  storage_encrypted = true

  db_name  = "agent_runtime"
  username = "runtime_admin"
  port     = 5432

  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.runtime.name
  parameter_group_name   = aws_db_parameter_group.runtime.name
  vpc_security_group_ids = [aws_security_group.database.id]
  publicly_accessible    = false
  multi_az               = false

  auto_minor_version_upgrade = false
  backup_retention_period    = var.db_backup_retention_days
  copy_tags_to_snapshot      = true
  deletion_protection        = var.rds_deletion_protection
  skip_final_snapshot        = var.skip_final_snapshot
  final_snapshot_identifier  = var.skip_final_snapshot ? null : var.final_snapshot_identifier

  enabled_cloudwatch_logs_exports = ["postgresql"]
  performance_insights_enabled    = false
  monitoring_interval             = 0

  lifecycle {
    precondition {
      condition     = var.db_allocated_storage_gib == 20
      error_message = "The first P6 proof remains capped at 20 GiB; broaden only with a reviewed cost change."
    }

    precondition {
      condition     = var.skip_final_snapshot || var.final_snapshot_identifier != null
      error_message = "A unique final_snapshot_identifier is required when the final RDS snapshot is retained."
    }
  }
}
