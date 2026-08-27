#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
lockfile_mode="${TF_LOCKFILE_MODE:-readonly}"
format_mode="${TF_FORMAT_MODE:-check}"

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN
unset AWS_PROFILE
unset AWS_WEB_IDENTITY_TOKEN_FILE
unset AWS_ROLE_ARN
export AWS_EC2_METADATA_DISABLED=true
export TF_IN_AUTOMATION=true

if [[ "${format_mode}" == "write" ]]; then
  terraform fmt -recursive "${repository_root}/deploy/aws"
else
  terraform fmt -check -recursive "${repository_root}/deploy/aws"
fi

for stack in bootstrap proof; do
  stack_path="${repository_root}/deploy/aws/${stack}"
  terraform -chdir="${stack_path}" init \
    -backend=false \
    -input=false \
    -lockfile="${lockfile_mode}"
  terraform -chdir="${stack_path}" validate -no-color
  terraform -chdir="${stack_path}" test -no-color
done

git -C "${repository_root}" diff --exit-code -- deploy/aws
