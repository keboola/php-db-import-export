#!/usr/bin/env bash
set -Eeuo pipefail

ENV_FILE=".env"
INSERT_MODE=prepend
VERBOSE=false

help () {
  echo "Syntax: update-env.sh [-v] [-a] [-e ${ENV_FILE}] <aws|azure|bigquery>"
  echo "Options:"
  echo "  -a|--append           Append mode (by default values are prepended to the env file)"
  echo "  -e|--env-file file    Env file to write (default: ${ENV_FILE})"
  echo "  -v|--verbose          Output extra information"
  echo ""
  echo "Example: update-env.sh aws"
  echo "Example: update-env.sh -e .env.local azure"
  echo ""
}

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -a|--append)
      INSERT_MODE=append
      shift
      ;;
    -e|--env-file)
      ENV_FILE="$2"
      shift
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      echo "Update env file with values from Terraform"
      echo ""
      help
      exit 0
      ;;
    -*|--*)
      echo "Unknown option $1"
      echo ""
      help
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done
set -- "${POSITIONAL_ARGS[@]}"

ENV_NAME=${1:-}
if [[ $ENV_NAME != "aws" && $ENV_NAME != "azure" && $ENV_NAME != "bigquery" ]]; then
    echo "Invalid environment name '${ENV_NAME}'. Possible values are: aws, azure, bigquery"
    echo ""
    help
    exit 1
fi

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_PATH}/.."
TF_DIR="${SCRIPT_PATH}/${ENV_NAME}"
cd "${PROJECT_ROOT}"

DELIMITER_START="##>> BEGIN ${ENV_NAME^^} GENERATED CONTENT <<##"
DELIMITER_END="##>> END ${ENV_NAME^^} GENERATED CONTENT <<##"

if [ ! -f "${ENV_FILE}" ]; then
  echo "Creating missing env file"
  touch "${ENV_FILE}"
fi

echo -e "Configuring \033[1;33m${ENV_FILE}\033[0m for \033[1;33m${ENV_NAME}\033[0m from \033[1;33m${TF_DIR}\033[0m"

if ! grep -q "${DELIMITER_START}" "${ENV_FILE}"; then
  if [[ $INSERT_MODE == "append" ]]; then
      echo "Appending new auto-generated section to env file"
      echo "" >> "${ENV_FILE}"
      echo "${DELIMITER_START}" >> "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
  else
      echo "Prepending new auto-generated section to env file"
      ENV=$(cat "${ENV_FILE}")
      echo "${DELIMITER_START}" > "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
      echo "" >> "${ENV_FILE}"
      echo "${ENV}" >> "${ENV_FILE}"
  fi
fi

if [ "${VERBOSE}" = true ]; then
  echo "Terraform outputs"
  terraform -chdir="${TF_DIR}" output
  echo ""
fi

echo "Building variables"
ENV_TF_FILE="${ENV_FILE}.tf"
TF_OUTPUTS_FILE="${TF_DIR}/tfoutput.json"
trap "rm -f ${TF_OUTPUTS_FILE}; rm -f ${ENV_TF_FILE}" EXIT
terraform -chdir="${TF_DIR}" output -json > "${TF_OUTPUTS_FILE}"

"${SCRIPT_PATH}/env-scripts/extract-variables-${ENV_NAME}.sh" "$TF_OUTPUTS_FILE" > "${ENV_TF_FILE}"

echo "Writing variables"

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
else
  sed -i'' -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
fi

echo "Done"
