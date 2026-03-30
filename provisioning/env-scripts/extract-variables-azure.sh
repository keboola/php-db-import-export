#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh $TF_OUTPUTS_FILE

output_var 'ABS_ACCOUNT_NAME' $(terraform_output 'ABS_ACCOUNT_NAME')
output_var 'ABS_ACCOUNT_KEY' $(terraform_output 'ABS_ACCOUNT_KEY')
output_var 'ABS_CONTAINER_NAME' $(terraform_output 'ABS_CONTAINER_NAME')
