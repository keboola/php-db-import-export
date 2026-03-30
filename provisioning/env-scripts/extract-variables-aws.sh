#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh $TF_OUTPUTS_FILE

output_var 'AWS_REGION' $(terraform_output 'AWS_REGION')
output_var 'AWS_ACCESS_KEY_ID' $(terraform_output 'AWS_ACCESS_KEY_ID')
output_var 'AWS_SECRET_ACCESS_KEY' $(terraform_output 'AWS_SECRET_ACCESS_KEY')
output_var 'AWS_S3_BUCKET' $(terraform_output 'AWS_S3_BUCKET')
output_var 'AWS_S3_KEY' 'exp-2'
