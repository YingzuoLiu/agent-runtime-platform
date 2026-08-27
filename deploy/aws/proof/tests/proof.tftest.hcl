mock_provider "aws" {
  override_during = plan

  mock_resource "aws_ecr_repository" {
    defaults = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/agent-runtime-platform-proof"
      name           = "agent-runtime-platform-proof"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/agent-runtime-platform-proof"
    }
  }

  mock_resource "aws_secretsmanager_secret" {
    defaults = {
      arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:runtime-dsn"
    }
  }

  mock_resource "aws_iam_openid_connect_provider" {
    defaults = {
      arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
    }
  }

  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/offline-proof-role"
    }
  }
}

run "offline_proof_topology" {
  command = plan

  variables {
    aws_account_id                    = "123456789012"
    aws_region                        = "us-east-1"
    availability_zones                = ["us-east-1a", "us-east-1b"]
    owner                              = "offline-proof"
    source_revision                    = "c9202b6b5bd1f98430b3a93e6945ba9d8bc51032"
    image_digest                       = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    postgres_engine_version            = "16.10"
    skip_final_snapshot                = true
    runtime_secret_recovery_window_days = 7
    monthly_budget_usd                  = 25
    budget_alert_email                  = "owner@example.com"
  }

  assert {
    condition     = length(aws_subnet.runtime) == 2 && length(aws_subnet.database) == 2
    error_message = "The proof topology must span two explicit AZ subnet pairs."
  }

  assert {
    condition     = alltrue([for subnet in aws_subnet.database : subnet.map_public_ip_on_launch == false])
    error_message = "Database subnets must never map public IP addresses."
  }

  assert {
    condition = (
      aws_db_instance.runtime.publicly_accessible == false &&
      aws_db_instance.runtime.multi_az == false &&
      aws_db_instance.runtime.storage_encrypted == true
    )
    error_message = "RDS must be private and encrypted; the first proof must remain explicitly non-HA."
  }

  assert {
    condition = one([
      for parameter in aws_db_parameter_group.runtime.parameter : parameter.value
      if parameter.name == "rds.force_ssl"
    ]) == "1"
    error_message = "The RDS parameter group must force TLS."
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.database_postgres.from_port == 5432 &&
      aws_vpc_security_group_ingress_rule.database_postgres.to_port == 5432 &&
      aws_vpc_security_group_ingress_rule.database_postgres.referenced_security_group_id == aws_security_group.runtime.id
    )
    error_message = "RDS may accept PostgreSQL only from the Runtime security group."
  }

  assert {
    condition = (
      aws_ecr_repository.runtime.image_tag_mutability == "IMMUTABLE" &&
      aws_ecr_repository.runtime.image_scanning_configuration[0].scan_on_push == true
    )
    error_message = "The release registry must reject tag mutation and scan on push."
  }

  assert {
    condition     = aws_ecs_service.runtime.desired_count == 0
    error_message = "Offline/default planning must not schedule a Runtime task."
  }

  assert {
    condition = endswith(
      jsondecode(aws_ecs_task_definition.runtime.container_definitions)[0].image,
      "@sha256:1111111111111111111111111111111111111111111111111111111111111111"
    )
    error_message = "Runtime tasks must select an immutable image digest."
  }

  assert {
    condition = one([
      for item in jsondecode(aws_ecs_task_definition.runtime.container_definitions)[0].environment : item.value
      if item.name == "RUNTIME_STORE_BACKEND"
    ]) == "postgres"
    error_message = "Every deployed Runtime task must select PostgreSQL authority."
  }

  assert {
    condition = one([
      for item in jsondecode(aws_ecs_task_definition.runtime.container_definitions)[0].environment : item.value
      if item.name == "RUNTIME_EXTERNAL_ACTION_MODE"
    ]) == "disabled"
    error_message = "P6 must keep external writes disabled."
  }

  assert {
    condition = one([
      for item in jsondecode(aws_ecs_task_definition.runtime.container_definitions)[0].secrets : item.name
      if item.name == "RUNTIME_POSTGRES_DSN"
    ]) == "RUNTIME_POSTGRES_DSN"
    error_message = "The PostgreSQL DSN must enter through ECS secret injection, not plaintext environment configuration."
  }

  assert {
    condition = jsondecode(aws_ecs_task_definition.bootstrap.container_definitions)[0].command == [
      "python",
      "-m",
      "runtime_service.postgres_bootstrap",
      "--apply",
    ]
    error_message = "Schema mutation must remain a separate one-off bootstrap task."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.github_release.assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:aud"] == "sts.amazonaws.com" &&
      jsondecode(aws_iam_role.github_release.assume_role_policy).Statement[0].Condition.StringEquals["token.actions.githubusercontent.com:sub"] == "repo:YingzuoLiu/agent-runtime-platform:environment:proof"
    )
    error_message = "GitHub OIDC trust must bind the exact audience, repository, and proof Environment."
  }

  assert {
    condition = (
      output.network_posture.high_availability == false &&
      output.network_posture.load_balancer == false &&
      output.network_posture.nat_gateway == false &&
      output.network_posture.runtime_inbound_rules == 0
    )
    error_message = "The topology must preserve its non-HA/no-ingress/no-NAT claim boundary."
  }
}

run "unseeded_secret_blocks_runtime_start" {
  command = plan

  variables {
    aws_account_id                    = "123456789012"
    aws_region                        = "us-east-1"
    availability_zones                = ["us-east-1a", "us-east-1b"]
    owner                              = "offline-proof"
    source_revision                    = "c9202b6b5bd1f98430b3a93e6945ba9d8bc51032"
    image_digest                       = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    postgres_engine_version            = "16.10"
    skip_final_snapshot                = true
    runtime_secret_recovery_window_days = 7
    monthly_budget_usd                  = 25
    budget_alert_email                  = "owner@example.com"
    service_desired_count               = 1
    runtime_secret_is_seeded            = false
  }

  expect_failures = [aws_ecs_service.runtime]
}

run "short_ecs_stop_budget_is_rejected" {
  command = plan

  variables {
    aws_account_id                    = "123456789012"
    aws_region                        = "us-east-1"
    availability_zones                = ["us-east-1a", "us-east-1b"]
    owner                              = "offline-proof"
    source_revision                    = "c9202b6b5bd1f98430b3a93e6945ba9d8bc51032"
    image_digest                       = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    postgres_engine_version            = "16.10"
    skip_final_snapshot                = true
    runtime_secret_recovery_window_days = 7
    monthly_budget_usd                  = 25
    budget_alert_email                  = "owner@example.com"
    ecs_stop_timeout_seconds            = 15
  }

  expect_failures = [aws_ecs_service.runtime]
}
