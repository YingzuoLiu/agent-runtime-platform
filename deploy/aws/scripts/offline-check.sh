#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
lockfile_mode="${TF_LOCKFILE_MODE:-update}"

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN
unset AWS_PROFILE
unset AWS_WEB_IDENTITY_TOKEN_FILE
unset AWS_ROLE_ARN
export AWS_EC2_METADATA_DISABLED=true
export TF_IN_AUTOMATION=true

terraform fmt -check -recursive "${repository_root}/deploy/aws"

for stack in bootstrap proof; do
  stack_path="${repository_root}/deploy/aws/${stack}"
  terraform -chdir="${stack_path}" init \
    -backend=false \
    -input=false \
    -lockfile="${lockfile_mode}"
  terraform -chdir="${stack_path}" validate -no-color
  terraform -chdir="${stack_path}" test -no-color
done
