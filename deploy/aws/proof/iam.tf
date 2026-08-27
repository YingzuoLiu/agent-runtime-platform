locals {
  ecs_tasks_assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.existing_github_oidc_provider_arn == null ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  tags = {
    Name = "github-actions"
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${local.name}-task-execution"
  assume_role_policy = local.ecs_tasks_assume_role_policy
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:${var.aws_partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "task_execution_secret" {
  name = "runtime-secret-read"
  role = aws_iam_role.task_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadRuntimePostgresDsn"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.runtime_postgres_dsn.arn
      },
    ]
  })
}

resource "aws_iam_role" "runtime_task" {
  name               = "${local.name}-runtime-task"
  assume_role_policy = local.ecs_tasks_assume_role_policy

  # Intentionally no AWS permissions: application correctness uses PostgreSQL,
  # ordinary HTTP, and injected configuration rather than an AWS API.
}

resource "aws_iam_role" "bootstrap_task" {
  name               = "${local.name}-bootstrap-task"
  assume_role_policy = local.ecs_tasks_assume_role_policy

  # Database bootstrap receives the same DSN through ECS secret injection and
  # has no authority to mutate AWS control-plane resources.
}

resource "aws_iam_role" "github_release" {
  name                 = "${local.name}-github-release"
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = local.github_oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
            "token.actions.githubusercontent.com:sub" = local.github_oidc_subject
          }
        }
      },
    ]
  })

  lifecycle {
    precondition {
      condition = (
        var.existing_github_oidc_provider_arn == null ||
        strcontains(var.existing_github_oidc_provider_arn, "::${var.aws_account_id}:")
      )
      error_message = "The supplied GitHub OIDC provider must belong to aws_account_id."
    }
  }
}

resource "aws_iam_role_policy" "github_release" {
  name = "immutable-runtime-release"
  role = aws_iam_role.github_release.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "GetEcrAuthorization"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Sid    = "PushAndRereadRuntimeImage"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:DescribeImages",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart",
        ]
        Resource = local.ecr_repository_arn
      },
      {
        Sid    = "RegisterAndInspectTaskDefinitions"
        Effect = "Allow"
        Action = [
          "ecs:DescribeTaskDefinition",
          "ecs:RegisterTaskDefinition",
        ]
        Resource = "*"
      },
      {
        Sid    = "UpdateProofService"
        Effect = "Allow"
        Action = [
          "ecs:DescribeServices",
          "ecs:UpdateService",
        ]
        Resource = local.ecs_service_arn
      },
      {
        Sid      = "RunVersionedProofTasks"
        Effect   = "Allow"
        Action   = ["ecs:RunTask"]
        Resource = local.task_family_arn
        Condition = {
          ArnEquals = {
            "ecs:cluster" = local.ecs_cluster_arn
          }
        }
      },
      {
        Sid    = "InspectProofTasks"
        Effect = "Allow"
        Action = [
          "ecs:DescribeTasks",
          "ecs:ListTasks",
        ]
        Resource = "*"
        Condition = {
          ArnEquals = {
            "ecs:cluster" = local.ecs_cluster_arn
          }
        }
      },
      {
        Sid    = "PassOnlyProofTaskRoles"
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.task_execution.arn,
          aws_iam_role.runtime_task.arn,
          aws_iam_role.bootstrap_task.arn,
        ]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      {
        Sid    = "ReadReleaseEvidence"
        Effect = "Allow"
        Action = [
          "logs:FilterLogEvents",
          "logs:GetLogEvents",
          "logs:StartQuery",
          "logs:GetQueryResults",
        ]
        Resource = "arn:${var.aws_partition}:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/ecs/${local.name}:*"
      },
      {
        Sid      = "InspectSecretMetadataOnly"
        Effect   = "Allow"
        Action   = ["secretsmanager:DescribeSecret"]
        Resource = aws_secretsmanager_secret.runtime_postgres_dsn.arn
      },
      {
        Sid      = "InspectDatabaseMetadata"
        Effect   = "Allow"
        Action   = ["rds:DescribeDBInstances"]
        Resource = "*"
      },
    ]
  })
}
