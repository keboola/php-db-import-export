# DB Import export library

## Supported operations

- Load/Import csv from `ABS` to `Snowflake`
- Load/Import csv from `GCS` to `Bigquery`
- Unload/Export table from `Snowflake` to `ABS`

## Features

### Import
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines
- Convert empty values to NULL (using convertEmptyValuesToNull option)

## Export
- Full unload - destination csv is always rewriten

## Development

### Prerequisites

- Docker
- Terraform
- CLI tools for the backends you want to test:
  - `aws` CLI ([setup guide](https://keboola.atlassian.net/wiki/spaces/KB/pages/2559475718/AWS+CLI#Using-named-profiles))
  - `az` CLI
  - `gcloud` CLI

### Quick start

```bash
cp .env.dist .env
docker compose build
```

You only need to provision the backends you want to test. Each backend has its own independent Terraform configuration in `provisioning/`.

### Provisioning cloud resources

Each provider is independent — provision only what you need.

#### AWS (S3)

```bash
aws sso login --profile=Keboola-Dev-Connection-Team-AWSAdministratorAccess

# Set your prefix (keep it short, e.g. your nick)
cat <<EOF > ./provisioning/aws/terraform.tfvars
name_prefix = "<your-nick>"
EOF

terraform -chdir=./provisioning/aws init
terraform -chdir=./provisioning/aws apply

# Export credentials to .env
./provisioning/update-env.sh aws
```

For CI, override the AWS account:

```bash
terraform -chdir=./provisioning/aws apply -var aws_account_id=149899208592 -var aws_profile=<ci-profile>
```

Load test fixtures:

```bash
docker compose run --rm dev composer loadS3
```

#### Azure (ABS)

```bash
az login
az account set --subscription eac4eb61-1abe-47e2-a0a1-f0a7e066f385

cat <<EOF > ./provisioning/azure/terraform.tfvars
name_prefix = "<your-nick>"
EOF

terraform -chdir=./provisioning/azure init
terraform -chdir=./provisioning/azure apply

./provisioning/update-env.sh azure
```

Load test fixtures:

```bash
docker compose run --rm dev composer loadAbs
```

#### BigQuery

Requires `resourcemanager.folders.create` permission for the organization.

```bash
gcloud auth application-default login

cat <<EOF > ./provisioning/bigquery/terraform.tfvars
name_prefix        = "<your-nick>"
folder_id          = "<GCP folder ID from https://console.cloud.google.com/cloud-resource-manager>"
billing_account_id = "<billing account ID from https://console.cloud.google.com/billing/>"
EOF

terraform -chdir=./provisioning/bigquery init
terraform -chdir=./provisioning/bigquery apply

./provisioning/update-env.sh bigquery
```

Load test fixtures:

```bash
docker compose run --rm dev composer loadGcs-bigquery
```

#### Snowflake

Snowflake resources are created manually. On `keboolaconnectiondev.us-east-1.snowflakecomputing.com`:

```sql
CREATE ROLE "<PREFIX>_DB_IMPORT_EXPORT";
CREATE DATABASE "<PREFIX>_DB_IMPORT_EXPORT";

GRANT ALL PRIVILEGES ON DATABASE "<PREFIX>_DB_IMPORT_EXPORT" TO ROLE "<PREFIX>_DB_IMPORT_EXPORT";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "<PREFIX>_DB_IMPORT_EXPORT";

CREATE USER "<PREFIX>_DB_IMPORT_EXPORT"
  PASSWORD = '<password>'
  DEFAULT_ROLE = "<PREFIX>_DB_IMPORT_EXPORT";

GRANT ROLE "<PREFIX>_DB_IMPORT_EXPORT" TO USER "<PREFIX>_DB_IMPORT_EXPORT";
```

Set env variables in `.env`:

```
SNOWFLAKE_HOST=keboolaconnectiondev.us-east-1.snowflakecomputing.com
SNOWFLAKE_PORT=443
SNOWFLAKE_USER=<PREFIX>_DB_IMPORT_EXPORT
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_DATABASE=<PREFIX>_DB_IMPORT_EXPORT
SNOWFLAKE_WAREHOUSE=DEV
```

For GCS-Snowflake tests, create a [storage integration](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html#creating-a-custom-iam-role):

```sql
CREATE STORAGE INTEGRATION "<PREFIX>_DB_IMPORT_EXPORT"
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<your-gcs-bucket>/');

-- Get STORAGE_GCP_SERVICE_ACCOUNT and grant it access to your GCS bucket
DESC STORAGE INTEGRATION "<PREFIX>_DB_IMPORT_EXPORT";
```

Set `GCS_INTEGRATION_NAME` in `.env`.

#### GCS (for Snowflake-GCS tests)

If you need GCS staging for Snowflake (not BigQuery), set up manually:

1. Create a GCS bucket
2. Create a service account with Storage Admin role on the bucket
3. Generate a JSON key and set `GCS_CREDENTIALS` in `.env` (`cat key.json | jq -c`)
4. Set `GCS_BUCKET_NAME` in `.env`
5. Load fixtures: `docker compose run --rm dev composer loadGcs-snowflake`

### Tests

```bash
# All tests (requires all backends configured)
docker compose run --rm dev composer tests

# Unit tests only (no backend needed)
docker compose run --rm dev composer tests-unit

# Backend-specific functional tests
docker compose run --rm dev composer tests-snowflake-abs
docker compose run --rm dev composer tests-snowflake-s3
docker compose run --rm dev composer tests-snowflake-gcs
docker compose run --rm dev composer tests-bigquery
```

### Code quality

```bash
docker compose run --rm dev composer phplint
docker compose run --rm dev composer phpcs
docker compose run --rm dev composer phpstan
```

### Full CI workflow

Runs all checks, loads fixtures, and runs tests:

```bash
docker compose run --rm dev composer ci
```


### Usage

#### Snowflake

ABS -> Snowflake `import/load`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

$absSourceFile = new Storage\ABS\SourceFile(...);
$snowflakeDestinationTable = new Storage\Snowflake\Table(...);
$importOptions = new ImportOptions(...);

(new Importer($snowflakeConnection))->importTable(
    $absSourceFile,
    $snowflakeDestinationTable,
    $importOptions
);
```

Snowflake -> Snowflake `copy`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

$snowflakeSourceTable = new Storage\Snowflake\Table(...);
$snowflakeDestinationTable = new Storage\Snowflake\Table(...);
$importOptions = new ImportOptions(...);

(new Importer($snowflakeConnection))->importTable(
    $snowflakeSourceTable,
    $snowflakeDestinationTable,
    $importOptions
);
```

Snowflake -> ABS `export/unload`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

$snowflakeSourceTable = new Storage\Snowflake\Table(...);
$absDestinationFile = new Storage\ABS\DestinationFile(...);
$exportOptions = new ExportOptions(...);

(new Exporter($snowflakeConnection))->exportTable(
    $snowflakeSourceTable,
    $absDestinationFile,
    $exportOptions
);
```

### Internals/Extending

Library consists of few simple interfaces.

#### Create new backend

Importer, Exporter Interface must be implemented in new Backed
```
Keboola\Db\ImportExport\Backend\ImporterInterface
Keboola\Db\ImportExport\Backend\ExporterInterface
```

For each backend there is corresponding adapter which supports own combination of SourceInterface and DestinationInterface. Custom adapters can be set with `setAdapters` method.

#### Create new storage

Storage is now file storage ABS|S3 (in future) or table storage Snowflake.
Storage can have `Source` and `Destination` which must implement `SourceInterface` or `DestinationInterface`. These interfaces are empty and it's up to adapter to support own combination.
In general there is one Import/Export adapter per FileStorage <=> TableStorage combination.

Adapter must implement:
- `Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface` for import
- `Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface` for export

Backend can require own extended AdapterInterface.



## License

MIT licensed, see [LICENSE](./LICENSE) file.
