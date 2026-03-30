#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh $TF_OUTPUTS_FILE

output_var 'BQ_KEY_FILE' "$(terraform_output 'BQ_KEY_FILE' | jq -c)"
output_var 'BQ_BUCKET_NAME' $(terraform_output 'BQ_BUCKET_NAME')
