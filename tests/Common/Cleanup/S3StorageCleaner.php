<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

use Aws\S3\S3Client;
use DateTimeInterface;
use Throwable;

/**
 * Deletes fixture objects left behind by previous CI runs.
 *
 * Same age-based reasoning as GcsStorageCleaner: fixtures are prefixed per run
 * (see FixturePath), so only the TTL can tell an orphan from a live run's file.
 */
final class S3StorageCleaner
{
    public function __construct(
        private readonly S3Client $client,
        private readonly string $bucket,
    ) {
    }

    public static function fromEnv(): self
    {
        return new self(
            new S3Client([
                'credentials' => [
                    'key' => (string) getenv('AWS_ACCESS_KEY_ID'),
                    'secret' => (string) getenv('AWS_SECRET_ACCESS_KEY'),
                ],
                'region' => (string) getenv('AWS_REGION'),
                'version' => '2006-03-01',
            ]),
            (string) getenv('AWS_S3_BUCKET'),
        );
    }

    public function cleanOlderThan(int $ttlSeconds): void
    {
        $threshold = time() - $ttlSeconds;

        $deleted = 0;
        $paginator = $this->client->getPaginator('ListObjectsV2', ['Bucket' => $this->bucket]);
        foreach ($paginator as $page) {
            /** @var array<int, array{Key: string, LastModified?: DateTimeInterface}> $objects */
            $objects = $page->get('Contents') ?? [];
            $staleKeys = [];
            foreach ($objects as $object) {
                if (!StaleFixturePrefix::isFixtureOfSomeRun($object['Key'])) {
                    continue;
                }
                $lastModified = $object['LastModified'] ?? null;
                if (!$lastModified instanceof DateTimeInterface || $lastModified->getTimestamp() > $threshold) {
                    // Unknown age or too new - could belong to a live run, leave it alone.
                    continue;
                }
                $staleKeys[] = ['Key' => $object['Key']];
            }

            if ($staleKeys === []) {
                continue;
            }

            try {
                $this->client->deleteObjects([
                    'Bucket' => $this->bucket,
                    'Delete' => ['Objects' => $staleKeys],
                ]);
                $deleted += count($staleKeys);
            } catch (Throwable $e) {
                // Best effort - objects another run is deleting concurrently are fine to skip.
                printf("[cleanup:s3] skip batch of %d: %s\n", count($staleKeys), $e->getMessage());
            }
        }

        printf("[cleanup:s3] done, deleted %d stale object(s) from %s\n", $deleted, $this->bucket);
    }
}
