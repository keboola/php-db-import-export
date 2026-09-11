<?php

declare(strict_types=1);

/**
 * Deletes stale test schemas/datasets and fixture files from the shared CI
 * backends by age.
 *
 * Run as a pre-test CI step so orphans from cancelled/parallel runs are reaped
 * without a standalone schedule and without touching the per-test lifecycle.
 * Age-gated (TEST_OBJECT_TTL_HOURS, default 6) so it never removes objects a
 * concurrent run is still using. Best-effort: it must never block the tests.
 *
 * Usage: php cleanup.php snowflake|bigquery|abs|s3|gcs-snowflake|gcs-bigquery
 */

use Tests\Keboola\Db\ImportExportCommon\Cleanup\AbsStorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\BigQueryCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\GcsStorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\S3StorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\SnowflakeCleaner;

date_default_timezone_set('Europe/Prague');
ini_set('display_errors', '1');
error_reporting(E_ALL);

$basedir = dirname(__DIR__);

require_once $basedir . '/../../vendor/autoload.php';

$backend = $argv[1] ?? '';
$ttlHours = (int) (getenv('TEST_OBJECT_TTL_HOURS') ?: '6');
$ttlSeconds = max(1, $ttlHours) * 3600;

try {
    switch ($backend) {
        case 'snowflake':
            SnowflakeCleaner::fromEnv()->cleanOlderThan($ttlSeconds);
            break;
        case 'bigquery':
            BigQueryCleaner::fromEnv()->cleanOlderThan($ttlSeconds);
            break;
        case 'abs':
            AbsStorageCleaner::fromEnv()->cleanOlderThan($ttlSeconds);
            break;
        case 's3':
            S3StorageCleaner::fromEnv()->cleanOlderThan($ttlSeconds);
            break;
        case 'gcs-snowflake':
            GcsStorageCleaner::fromEnv('GCS_CREDENTIALS', 'GCS_BUCKET_NAME')->cleanOlderThan($ttlSeconds);
            break;
        case 'gcs-bigquery':
            GcsStorageCleaner::fromEnv('BQ_KEY_FILE', 'BQ_BUCKET_NAME')->cleanOlderThan($ttlSeconds);
            break;
        default:
            fwrite(
                STDERR,
                "Usage: cleanup.php snowflake|bigquery|abs|s3|gcs-snowflake|gcs-bigquery\n",
            );
            exit(2);
    }
} catch (Throwable $e) {
    // Best effort - a cleanup failure must never fail the build.
    fwrite(STDERR, sprintf("[cleanup] non-fatal error: %s: %s\n", get_class($e), $e->getMessage()));
    exit(0);
}
