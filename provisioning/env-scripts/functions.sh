#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

terraform_output() {
  jq ".${1}.value" -r $TF_OUTPUTS_FILE
}

output_var() {
  echo "${1}=\"${2}\""
}
