<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

use Keboola\FileStorage\Abs\ClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Throwable;

/**
 * Deletes fixture blobs left behind by previous CI runs.
 *
 * Same age-based reasoning as GcsStorageCleaner: fixtures are prefixed per run
 * (see FixturePath), so only the TTL can tell an orphan from a live run's blob.
 */
final class AbsStorageCleaner
{
    public function __construct(
        private readonly BlobRestProxy $client,
        private readonly string $containerName,
    ) {
    }

    public static function fromEnv(): self
    {
        $connectionString = sprintf(
            'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net',
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY'),
        );

        return new self(
            ClientFactory::createClientFromConnectionString($connectionString),
            (string) getenv('ABS_CONTAINER_NAME'),
        );
    }

    public function cleanOlderThan(int $ttlSeconds): void
    {
        $threshold = time() - $ttlSeconds;

        $deleted = 0;
        $listOptions = new ListBlobsOptions();
        do {
            $result = $this->client->listBlobs($this->containerName, $listOptions);
            /** @var Blob $blob */
            foreach ($result->getBlobs() as $blob) {
                if (!StaleFixturePrefix::isFixtureOfSomeRun($blob->getName())) {
                    continue;
                }

                if ($blob->getProperties()->getLastModified()->getTimestamp() > $threshold) {
                    // Too new - could belong to a live run, leave it alone.
                    continue;
                }

                try {
                    $this->client->deleteBlob($this->containerName, $blob->getName());
                    $deleted++;
                } catch (Throwable $e) {
                    // Best effort - a blob another run is deleting concurrently is fine to skip.
                    printf("[cleanup:abs] skip %s: %s\n", $blob->getName(), $e->getMessage());
                }
            }
            $nextMarker = $result->getNextMarker();
            $listOptions->setMarker($nextMarker);
        } while ($nextMarker !== '');

        printf("[cleanup:abs] done, deleted %d stale blob(s) from %s\n", $deleted, $this->containerName);
    }
}
