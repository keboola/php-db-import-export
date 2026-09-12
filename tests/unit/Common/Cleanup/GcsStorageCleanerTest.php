<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common\Cleanup;

use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\GcsStorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\StaleFixturePrefix;

class GcsStorageCleanerTest extends TestCase
{
    private const BUCKET_NAME = 'test-fixtures-bucket';

    public function testDeletesStaleObjectMatchingRunPrefix(): void
    {
        $staleObject = $this->createMock(StorageObject::class);
        $staleObject->method('name')->willReturn('ci-123/tests-bigquery/x.csv');
        $staleObject->method('info')->willReturn(['updated' => gmdate('c', time() - 3600)]);
        $staleObject->expects(self::once())->method('delete');

        $output = $this->runCleaner([$staleObject], 60);

        self::assertStringContainsString('[cleanup:gcs] done, deleted 1 stale object(s)', $output);
    }

    public function testKeepsObjectWithinTtl(): void
    {
        $freshObject = $this->createMock(StorageObject::class);
        $freshObject->method('name')->willReturn('ci-456/tests-bigquery/fresh.csv');
        $freshObject->method('info')->willReturn(['updated' => gmdate('c', time() - 5)]);
        $freshObject->expects(self::never())->method('delete');

        $output = $this->runCleaner([$freshObject], 3600);

        self::assertStringContainsString('[cleanup:gcs] done, deleted 0 stale object(s)', $output);
    }

    public function testIgnoresObjectNotMatchingRunPrefixRegardlessOfAge(): void
    {
        $foreignObject = $this->createMock(StorageObject::class);
        $foreignObject->method('name')->willReturn('manual-upload/tests-bigquery/x.csv');
        $foreignObject->expects(self::never())->method('info');
        $foreignObject->expects(self::never())->method('delete');

        $output = $this->runCleaner([$foreignObject], 60);

        self::assertStringContainsString('[cleanup:gcs] done, deleted 0 stale object(s)', $output);
    }

    /**
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function unknownAgeInfoProvider(): iterable
    {
        yield 'missing updated key' => [[]];
        yield 'non-string updated value' => [['updated' => 12345]];
        yield 'unparseable updated string' => [['updated' => 'not-a-real-timestamp']];
    }

    /**
     * @param array<string, mixed> $info
     */
    #[DataProvider('unknownAgeInfoProvider')]
    public function testSkipsDeletionWhenAgeCannotBeDetermined(array $info): void
    {
        $object = $this->createMock(StorageObject::class);
        $object->method('name')->willReturn('ci-789/tests-bigquery/unknown-age.csv');
        $object->method('info')->willReturn($info);
        $object->expects(self::never())->method('delete');

        $output = $this->runCleaner([$object], 60);

        self::assertStringContainsString('[cleanup:gcs] done, deleted 0 stale object(s)', $output);
    }

    public function testSwallowsDeleteFailureAndContinuesSweep(): void
    {
        $failingObject = $this->createMock(StorageObject::class);
        $failingObject->method('name')->willReturn('ci-111/tests-bigquery/raced-delete.csv');
        $failingObject->method('info')->willReturn(['updated' => gmdate('c', time() - 3600)]);
        $failingObject->expects(self::once())->method('delete')
            ->willThrowException(new RuntimeException('object not found'));

        $output = $this->runCleaner([$failingObject], 60);

        self::assertStringContainsString(
            '[cleanup:gcs] skip ci-111/tests-bigquery/raced-delete.csv: object not found',
            $output,
        );
        self::assertStringContainsString('[cleanup:gcs] done, deleted 0 stale object(s)', $output);
    }

    public function testSwallowsInfoFailureAndStillDeletesLaterStaleObject(): void
    {
        $racedObject = $this->createMock(StorageObject::class);
        $racedObject->method('name')->willReturn('ci-222/tests-bigquery/raced-info.csv');
        $racedObject->method('info')->willThrowException(new RuntimeException('404 Not Found'));
        $racedObject->expects(self::never())->method('delete');

        $laterStaleObject = $this->createMock(StorageObject::class);
        $laterStaleObject->method('name')->willReturn('ci-333/tests-bigquery/later-stale.csv');
        $laterStaleObject->method('info')->willReturn(['updated' => gmdate('c', time() - 3600)]);
        $laterStaleObject->expects(self::once())->method('delete');

        $output = $this->runCleaner([$racedObject, $laterStaleObject], 60);

        self::assertStringContainsString(
            '[cleanup:gcs] skip ci-222/tests-bigquery/raced-info.csv: 404 Not Found',
            $output,
        );
        self::assertStringContainsString('[cleanup:gcs] done, deleted 1 stale object(s)', $output);
    }

    /**
     * @param list<StorageObject> $objects
     */
    private function runCleaner(array $objects, int $ttlSeconds): string
    {
        // Pure stubs: nothing here needs call-count verification, only StorageObject::delete() does.
        $bucket = $this->createMock(Bucket::class);
        $bucket->expects(self::once())
            ->method('objects')
            ->with(['prefix' => StaleFixturePrefix::RUN_PREFIX])
            ->willReturn($objects);

        $client = $this->createStub(StorageClient::class);
        $client->method('bucket')->willReturn($bucket);

        $cleaner = new GcsStorageCleaner($client, self::BUCKET_NAME);

        ob_start();
        $cleaner->cleanOlderThan($ttlSeconds);

        return (string) ob_get_clean();
    }
}
