output "backend_config" {
  description = "Non-secret backend values for the proof stack. S3 native lockfiles replace a DynamoDB lock table."
  value = {
    bucket       = aws_s3_bucket.terraform_state.bucket
    key          = "${var.project_name}/${var.environment}/terraform.tfstate"
    region       = var.aws_region
    use_lockfile = true
    encrypt      = true
  }
}

output "state_bucket_arn" {
  description = "State bucket retained until proof evidence and state versions are deliberately removed."
  value       = aws_s3_bucket.terraform_state.arn
}
