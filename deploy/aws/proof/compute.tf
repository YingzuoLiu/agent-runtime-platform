locals {
  runtime_environment = [
    for name, value in {
      RUNTIME_EXTERNAL_ACTION_MODE             = "disabled"
      RUNTIME_HTTP_CONCURRENCY_LIMIT           = tostring(var.runtime_http_concurrency_limit)
      RUNTIME_IMAGE_DIGEST                     = var.image_digest
      RUNTIME_LOG_LEVEL                        = "info"
      RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS   = tostring(var.manager_shutdown_grace_seconds)
      RUNTIME_RELEASE_IDENTITY_REQUIRED        = "true"
      RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS = tostring(var.server_shutdown_grace_seconds)
      RUNTIME_SOURCE_REVISION                  = var.source_revision
      RUNTIME_STORE_BACKEND                    = "postgres"
      RUNTIME_WORKER_COUNT                     = tostring(var.runtime_worker_count)
      } : {
      name  = name
      value = value
    }
  ]

  runtime_secrets = [
    {
      name      = "RUNTIME_POSTGRES_DSN"
      valueFrom = aws_secretsmanager_secret.runtime_postgres_dsn.arn
    },
  ]

  log_configuration = {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.runtime.name
      awslogs-region        = var.aws_region
      awslogs-stream-prefix = "runtime"
    }
  }
}

resource "aws_cloudwatch_log_group" "runtime" {
  name              = "/aws/ecs/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "runtime" {
  name = local.name

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_ecs_task_definition" "runtime" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.runtime_task.arn

  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name      = "runtime"
      image     = local.runtime_image
      essential = true
      user      = "10001"

      stopTimeout = var.ecs_stop_timeout_seconds

      portMappings = [
        {
          name          = "http"
          containerPort = 8000
          hostPort      = 8000
          protocol      = "tcp"
          appProtocol   = "http"
        },
      ]

      environment      = local.runtime_environment
      secrets          = local.runtime_secrets
      logConfiguration = local.log_configuration

      healthCheck = {
        command = [
          "CMD-SHELL",
          "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)\" || exit 1",
        ]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 15
      }
    },
  ])
}

resource "aws_ecs_task_definition" "bootstrap" {
  family                   = "${local.name}-bootstrap"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.bootstrap_task.arn

  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name      = "bootstrap"
      image     = local.runtime_image
      essential = true
      user      = "10001"

      command = [
        "python",
        "-m",
        "runtime_service.postgres_bootstrap",
        "--apply",
      ]

      environment = local.runtime_environment
      secrets     = local.runtime_secrets
      logConfiguration = merge(local.log_configuration, {
        options = merge(local.log_configuration.options, {
          awslogs-stream-prefix = "bootstrap"
        })
      })
    },
  ])
}

resource "aws_ecs_service" "runtime" {
  name             = local.name
  cluster          = aws_ecs_cluster.runtime.id
  task_definition  = aws_ecs_task_definition.runtime.arn
  desired_count    = var.service_desired_count
  launch_type      = "FARGATE"
  platform_version = var.fargate_platform_version

  enable_execute_command             = false
  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200
  propagate_tags                     = "SERVICE"
  wait_for_steady_state              = false

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    assign_public_ip = true
    security_groups  = [aws_security_group.runtime.id]
    subnets          = [for az in var.availability_zones : aws_subnet.runtime[az].id]
  }

  lifecycle {
    precondition {
      condition     = var.manager_shutdown_grace_seconds < var.server_shutdown_grace_seconds
      error_message = "RuntimeManager grace must be shorter than ASGI server grace."
    }

    precondition {
      condition     = var.server_shutdown_grace_seconds < var.ecs_stop_timeout_seconds
      error_message = "ECS stopTimeout must exceed the ASGI graceful-shutdown budget."
    }

    precondition {
      condition     = var.service_desired_count == 0 || var.runtime_secret_is_seeded
      error_message = "Runtime tasks cannot start until the DSN secret has a current value."
    }
  }
}
