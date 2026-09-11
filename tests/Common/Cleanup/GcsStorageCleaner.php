<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Throwable;

/**
 * Deletes fixture objects left behind by previous CI runs.
 *
 * Fixtures carry a run-unique prefix (see FixturePath), so a run only ever
 * clears its own files and a cancelled run orphans them. We cannot enumerate
 * other runs' ids, so we delete by AGE: an object written longer ago than the
 * TTL cannot belong to a live run.
 */
final class GcsStorageCleaner
{
    public function __construct(
        private readonly StorageClient $client,
        private readonly string $bucketName,
    ) {
    }

    public static function fromEnv(string $credentialsEnv, string $bucketEnv): self
    {
        /** @var array<string, mixed> $credentials */
        $credentials = json_decode((string) getenv($credentialsEnv), true, 512, JSON_THROW_ON_ERROR);

        return new self(
            new StorageClient(['keyFile' => $credentials]),
            (string) getenv($bucketEnv),
        );
    }

    public function cleanOlderThan(int $ttlSeconds): void
    {
        $threshold = time() - $ttlSeconds;
        $bucket = $this->client->bucket($this->bucketName);

        $deleted = 0;
        /** @var StorageObject $object */
        foreach ($bucket->objects() as $object) {
            if (!StaleFixturePrefix::isFixtureOfSomeRun($object->name())) {
                continue;
            }

            $info = $object->info();
            $updated = is_array($info) ? ($info['updated'] ?? null) : null;
            $updatedAt = is_string($updated) ? strtotime($updated) : false;
            if ($updatedAt === false || $updatedAt > $threshold) {
                // Unknown age or too new - could belong to a live run, leave it alone.
                continue;
            }

            try {
                $object->delete();
                $deleted++;
            } catch (Throwable $e) {
                // Best effort - an object another run is deleting concurrently is fine to skip.
                printf("[cleanup:gcs] skip %s: %s\n", $object->name(), $e->getMessage());
            }
        }

        printf("[cleanup:gcs] done, deleted %d stale object(s) from %s\n", $deleted, $this->bucketName);
    }
}
