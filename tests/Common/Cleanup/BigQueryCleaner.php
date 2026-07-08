<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

use Google\Cloud\BigQuery\Dataset;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Throwable;

/**
 * Deletes stale test datasets left behind by previous CI runs.
 *
 * Test datasets carry a run-unique BUILD_PREFIX (see BigqueryBaseTestCase), so
 * cancelled runs (cancel-in-progress) whose tearDown never fired orphan their
 * datasets. We cannot enumerate other runs' ids, so we delete by AGE: a dataset
 * created longer ago than the TTL cannot belong to a live run (a running job's
 * datasets are minutes old). BigQuery creationTime is an absolute epoch, so the
 * comparison needs no timezone handling.
 */
final class BigQueryCleaner
{
    /**
     * Base names of every dataset these tests create (before the _<buildPrefix>_<suite> suffix).
     * A dataset is only ever deleted if its name starts with one of these AND it is stale.
     */
    private const TEST_DATASET_PREFIXES = [
        'tests_source',
        'tests_destination',
        'ieLibTest',
        'import_export_test',
    ];

    public function __construct(private readonly BigQueryClientWrapper $client)
    {
    }

    public static function fromEnv(): self
    {
        /** @var array<string, mixed> $credentials */
        $credentials = json_decode((string) getenv('BQ_KEY_FILE'), true, 512, JSON_THROW_ON_ERROR);

        return new self(new BigQueryClientWrapper(
            ['keyFile' => $credentials],
            'cleanup-ie-lib',
        ));
    }

    public function cleanOlderThan(int $ttlSeconds): void
    {
        $thresholdMs = (time() - $ttlSeconds) * 1000;

        $deleted = 0;
        /** @var Dataset $dataset */
        foreach ($this->client->datasets() as $dataset) {
            $datasetId = $dataset->id();
            if (!self::isTestDataset($datasetId)) {
                continue;
            }

            $info = $dataset->info();
            $creationTime = is_array($info) ? ($info['creationTime'] ?? null) : null;
            if (!is_numeric($creationTime) || (int) $creationTime > $thresholdMs) {
                // Unknown age or too new - could belong to a live run, leave it alone.
                continue;
            }

            try {
                $dataset->delete(['deleteContents' => true]);
                $deleted++;
                printf("[cleanup:bigquery] deleted stale dataset %s\n", $datasetId);
            } catch (Throwable $e) {
                // Best effort - a dataset another run is deleting concurrently is fine to skip.
                printf("[cleanup:bigquery] skip %s: %s\n", $datasetId, $e->getMessage());
            }
        }

        printf("[cleanup:bigquery] done, deleted %d stale dataset(s)\n", $deleted);
    }

    private static function isTestDataset(string $name): bool
    {
        foreach (self::TEST_DATASET_PREFIXES as $prefix) {
            if (str_starts_with($name, $prefix)) {
                return true;
            }
        }

        return false;
    }
}
