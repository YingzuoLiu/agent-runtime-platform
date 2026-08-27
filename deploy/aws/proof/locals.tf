locals {
  name = "${var.project_name}-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = var.owner
    Purpose     = "operational-proof"
    Phase       = "P6"
    HighAvail   = "false"
  }

  ecr_repository_arn = "arn:${var.aws_partition}:ecr:${var.aws_region}:${var.aws_account_id}:repository/${local.name}"
  ecs_cluster_arn    = "arn:${var.aws_partition}:ecs:${var.aws_region}:${var.aws_account_id}:cluster/${local.name}"
  ecs_service_arn    = "arn:${var.aws_partition}:ecs:${var.aws_region}:${var.aws_account_id}:service/${local.name}/${local.name}"
  task_family_arn    = "arn:${var.aws_partition}:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${local.name}:*"

  github_oidc_subject = "repo:${var.github_repository}:environment:${var.github_environment}"
  github_oidc_provider_arn = (
    var.existing_github_oidc_provider_arn != null
    ? var.existing_github_oidc_provider_arn
    : aws_iam_openid_connect_provider.github[0].arn
  )

  runtime_image = "${aws_ecr_repository.runtime.repository_url}@${var.image_digest}"
}
