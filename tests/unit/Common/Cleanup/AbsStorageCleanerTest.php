<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common\Cleanup;

use DateTime;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\BlobProperties;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\AbsStorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\StaleFixturePrefix;

class AbsStorageCleanerTest extends TestCase
{
    private const CONTAINER = 'fixtures';

    public function testDeletesStaleFixtureBlob(): void
    {
        $blob = $this->createBlob('ci-123/tests-bigquery/x.csv', new DateTime('-2 hours'));

        $client = $this->createMock(BlobRestProxy::class);
        $client->expects(self::once())
            ->method('listBlobs')
            ->with(
                self::CONTAINER,
                self::callback(static fn(ListBlobsOptions $options): bool
                    => $options->getPrefix() === StaleFixturePrefix::RUN_PREFIX),
            )
            ->willReturn($this->createListBlobsResult([$blob], null));
        $client->expects(self::once())
            ->method('deleteBlob')
            ->with(self::CONTAINER, 'ci-123/tests-bigquery/x.csv');

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString('[cleanup:abs] done, deleted 1 stale blob(s) from fixtures', $output);
    }

    public function testKeepsBlobNewerThanTtl(): void
    {
        $blob = $this->createBlob('ci-123/tests-bigquery/x.csv', new DateTime('-1 second'));

        $client = $this->createMock(BlobRestProxy::class);
        $client->method('listBlobs')->willReturn($this->createListBlobsResult([$blob], null));
        $client->expects(self::never())->method('deleteBlob');

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString('[cleanup:abs] done, deleted 0 stale blob(s)', $output);
    }

    public function testIgnoresBlobsOutsideRunPrefixRegardlessOfAge(): void
    {
        $old = new DateTime('-2 hours');
        $blobs = [
            $this->createBlob('tests-bigquery/x.csv', $old),
            $this->createBlob('root-object.csv', $old),
        ];

        $client = $this->createMock(BlobRestProxy::class);
        $client->method('listBlobs')->willReturn($this->createListBlobsResult($blobs, null));
        $client->expects(self::never())->method('deleteBlob');

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString('[cleanup:abs] done, deleted 0 stale blob(s)', $output);
    }

    public function testUnknownLastModifiedIsSkippedWithoutAbortingTheSweep(): void
    {
        $blobs = [
            $this->createBlob('ci-123/unknown-age.csv', null),
            $this->createBlob('ci-123/stale.csv', new DateTime('-2 hours')),
        ];

        $client = $this->createMock(BlobRestProxy::class);
        $client->method('listBlobs')->willReturn($this->createListBlobsResult($blobs, null));
        $client->expects(self::once())
            ->method('deleteBlob')
            ->with(self::CONTAINER, 'ci-123/stale.csv');

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString('[cleanup:abs] done, deleted 1 stale blob(s)', $output);
    }

    public function testDeleteFailureIsSwallowedAndSweepContinues(): void
    {
        $blobs = [
            $this->createBlob('ci-123/fails.csv', new DateTime('-2 hours')),
            $this->createBlob('ci-123/succeeds.csv', new DateTime('-2 hours')),
        ];

        $client = $this->createMock(BlobRestProxy::class);
        $client->method('listBlobs')->willReturn($this->createListBlobsResult($blobs, null));
        $client->expects(self::exactly(2))
            ->method('deleteBlob')
            ->willReturnCallback(function (string $container, string $name): void {
                if ($name === 'ci-123/fails.csv') {
                    throw new RuntimeException('concurrent delete in progress');
                }
            });

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString(
            '[cleanup:abs] skip ci-123/fails.csv: concurrent delete in progress',
            $output,
        );
        self::assertStringContainsString('[cleanup:abs] done, deleted 1 stale blob(s)', $output);
    }

    public function testFollowsContinuationPagesUntilNullMarker(): void
    {
        $firstPageBlob = $this->createBlob('ci-123/page1.csv', new DateTime('-2 hours'));
        $secondPageBlob = $this->createBlob('ci-123/page2.csv', new DateTime('-2 hours'));

        // A regression checking for '' instead of null here would loop forever.
        $firstPage = $this->createListBlobsResult([$firstPageBlob], 'continuation-token');
        $secondPage = $this->createListBlobsResult([$secondPageBlob], null);

        $client = $this->createMock(BlobRestProxy::class);
        $client->expects(self::exactly(2))
            ->method('listBlobs')
            ->willReturnOnConsecutiveCalls($firstPage, $secondPage);
        $client->expects(self::exactly(2))->method('deleteBlob');

        $output = $this->runCleaner($client, 3600);

        self::assertStringContainsString('[cleanup:abs] done, deleted 2 stale blob(s)', $output);
    }

    private function createBlob(string $name, ?DateTime $lastModified): Blob
    {
        $properties = new BlobProperties();
        if ($lastModified !== null) {
            $properties->setLastModified($lastModified);
        }

        $blob = new Blob();
        $blob->setName($name);
        $blob->setProperties($properties);

        return $blob;
    }

    /**
     * @param Blob[] $blobs
     * @return ListBlobsResult&Stub
     */
    private function createListBlobsResult(array $blobs, ?string $nextMarker): ListBlobsResult
    {
        $result = $this->createStub(ListBlobsResult::class);
        $result->method('getBlobs')->willReturn($blobs);
        $result->method('getNextMarker')->willReturn($nextMarker);

        return $result;
    }

    /**
     * @param BlobRestProxy&MockObject $client
     */
    private function runCleaner(BlobRestProxy $client, int $ttlSeconds): string
    {
        $cleaner = new AbsStorageCleaner($client, self::CONTAINER);

        ob_start();
        $cleaner->cleanOlderThan($ttlSeconds);

        return (string) ob_get_clean();
    }
}
