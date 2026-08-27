mock_provider "aws" {}

run "state_backend_is_recoverable_and_private" {
  command = plan

  variables {
    aws_account_id = "123456789012"
    aws_region     = "us-east-1"
    owner          = "offline-proof"
  }

  assert {
    condition     = aws_s3_bucket.terraform_state.force_destroy == false
    error_message = "The state bucket must refuse implicit recursive deletion."
  }

  assert {
    condition     = aws_s3_bucket_versioning.terraform_state.versioning_configuration[0].status == "Enabled"
    error_message = "State recovery requires S3 versioning."
  }

  assert {
    condition = (
      aws_s3_bucket_public_access_block.terraform_state.block_public_acls &&
      aws_s3_bucket_public_access_block.terraform_state.block_public_policy &&
      aws_s3_bucket_public_access_block.terraform_state.ignore_public_acls &&
      aws_s3_bucket_public_access_block.terraform_state.restrict_public_buckets
    )
    error_message = "Every S3 public-access control must remain enabled."
  }

  assert {
    condition     = output.backend_config.use_lockfile && output.backend_config.encrypt
    error_message = "The proof backend must use S3 native locking and encryption."
  }
}
