<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common\Cleanup;

use Aws\MockHandler;
use Aws\Result;
use Aws\S3\S3Client;
use DateTimeImmutable;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\S3StorageCleaner;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\StaleFixturePrefix;

class S3StorageCleanerTest extends TestCase
{
    private const BUCKET = 'test-bucket';
    private const TTL_SECONDS = 3600;

    private string|false $originalKey;

    protected function setUp(): void
    {
        parent::setUp();
        $this->originalKey = getenv('AWS_S3_KEY');
        putenv('AWS_S3_KEY');
    }

    protected function tearDown(): void
    {
        if ($this->originalKey === false) {
            putenv('AWS_S3_KEY');
        } else {
            putenv(sprintf('AWS_S3_KEY=%s', $this->originalKey));
        }
        parent::tearDown();
    }

    public function testDeletesStaleFixtureObject(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/x.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                ],
                'IsTruncated' => false,
            ]),
            new Result([
                'Deleted' => [['Key' => 'ci-123/tests-bigquery/x.csv']],
                'Errors' => [],
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertSame('DeleteObjects', $handler->getLastCommand()->getName());
        $deleteRequest = $handler->getLastCommand()->toArray()['Delete'];
        self::assertIsArray($deleteRequest);
        self::assertSame([['Key' => 'ci-123/tests-bigquery/x.csv']], $deleteRequest['Objects']);
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 1 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testListingIsConstrainedToTheRunPrefixServerSide(): void
    {
        $handler = new MockHandler([
            new Result(['Contents' => [], 'IsTruncated' => false]),
        ]);
        $client = $this->createClient($handler);

        $this->runCleaner($client);

        self::assertSame(
            StaleFixturePrefix::RUN_PREFIX,
            $handler->getLastCommand()->toArray()['Prefix'],
        );
    }

    public function testListingPrefixIncludesTheConfiguredKeySubtree(): void
    {
        putenv('AWS_S3_KEY=some-key');

        $handler = new MockHandler([
            new Result(['Contents' => [], 'IsTruncated' => false]),
        ]);
        $client = $this->createClient($handler);

        $this->runCleaner($client);

        self::assertSame('some-key/' . StaleFixturePrefix::RUN_PREFIX, $handler->getLastCommand()->toArray()['Prefix']);
    }

    public function testLeavesFreshRunObjectAlone(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/fresh.csv', 'LastModified' => new DateTimeImmutable('-1 minute')],
                ],
                'IsTruncated' => false,
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        // No DeleteObjects was ever sent - the last (and only) command stays ListObjectsV2.
        self::assertSame('ListObjectsV2', $handler->getLastCommand()->getName());
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 0 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testLeavesForeignPathAloneRegardlessOfAge(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'manual/upload.csv', 'LastModified' => new DateTimeImmutable('-30 days')],
                ],
                'IsTruncated' => false,
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertSame('ListObjectsV2', $handler->getLastCommand()->getName());
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 0 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testLeavesUnknownAgeObjectsAlone(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/no-last-modified.csv'],
                    ['Key' => 'ci-123/tests-bigquery/string-last-modified.csv', 'LastModified' => 'not-a-date'],
                ],
                'IsTruncated' => false,
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertSame('ListObjectsV2', $handler->getLastCommand()->getName());
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 0 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testWalksAllPaginatorPagesAndDeletesStaleKeysFromEach(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/page1.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                ],
                'IsTruncated' => true,
                'NextContinuationToken' => 'token-1',
            ]),
            new Result([
                'Deleted' => [['Key' => 'ci-123/tests-bigquery/page1.csv']],
                'Errors' => [],
            ]),
            new Result([
                'Contents' => [
                    ['Key' => 'ci-456/tests-bigquery/page2.csv', 'LastModified' => new DateTimeImmutable('-3 days')],
                ],
                'IsTruncated' => false,
            ]),
            new Result([
                'Deleted' => [['Key' => 'ci-456/tests-bigquery/page2.csv']],
                'Errors' => [],
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertCount(0, $handler);
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 2 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testPartialDeleteObjectsFailureCountsOnlyActualDeletions(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/ok.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                    ['Key' => 'ci-123/tests-bigquery/denied.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                ],
                'IsTruncated' => false,
            ]),
            new Result([
                'Deleted' => [['Key' => 'ci-123/tests-bigquery/ok.csv']],
                'Errors' => [
                    [
                        'Key' => 'ci-123/tests-bigquery/denied.csv',
                        'Code' => 'AccessDenied',
                        'Message' => 'Access Denied',
                    ],
                ],
            ]),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertStringContainsString('[cleanup:s3] skip ci-123/tests-bigquery/denied.csv: Access Denied', $output);
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 1 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    public function testDeleteObjectsExceptionIsSwallowedAndRunFinishes(): void
    {
        $handler = new MockHandler([
            new Result([
                'Contents' => [
                    ['Key' => 'ci-123/tests-bigquery/a.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                    ['Key' => 'ci-123/tests-bigquery/b.csv', 'LastModified' => new DateTimeImmutable('-2 days')],
                ],
                'IsTruncated' => false,
            ]),
            new RuntimeException('simulated network blip'),
        ]);
        $client = $this->createClient($handler);

        $output = $this->runCleaner($client);

        self::assertStringContainsString('[cleanup:s3] skip batch of 2: simulated network blip', $output);
        self::assertStringContainsString(
            sprintf('[cleanup:s3] done, deleted 0 stale object(s) from %s', self::BUCKET),
            $output,
        );
    }

    private function createClient(MockHandler $handler): S3Client
    {
        return new S3Client([
            'region' => 'us-east-1',
            'version' => '2006-03-01',
            'credentials' => ['key' => 'test-key', 'secret' => 'test-secret'],
            'handler' => $handler,
        ]);
    }

    private function runCleaner(S3Client $client): string
    {
        $cleaner = new S3StorageCleaner($client, self::BUCKET);

        ob_start();
        $cleaner->cleanOlderThan(self::TTL_SECONDS);

        return (string) ob_get_clean();
    }
}
