<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common\Cleanup;

use PHPUnit\Framework\TestCase;
use Tests\Keboola\Db\ImportExportCommon\Cleanup\StaleFixturePrefix;

class StaleFixturePrefixTest extends TestCase
{
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

    public function testRootMatcherAcceptsRunPrefix(): void
    {
        self::assertTrue(StaleFixturePrefix::isFixtureOfSomeRun('ci-123/tests-bigquery/file.csv'));
    }

    public function testRootMatcherRejectsForeignPath(): void
    {
        self::assertFalse(StaleFixturePrefix::isFixtureOfSomeRun('tests-bigquery/file.csv'));
        self::assertFalse(StaleFixturePrefix::isFixtureOfSomeRun('ci-abc/file.csv'));
        self::assertFalse(StaleFixturePrefix::isFixtureOfSomeRun('nested/ci-123/file.csv'));
    }

    public function testRootMatcherIgnoresS3Key(): void
    {
        putenv('AWS_S3_KEY=some-key');

        self::assertFalse(StaleFixturePrefix::isFixtureOfSomeRun('some-key/ci-123/file.csv'));
    }

    public function testS3MatcherAcceptsRunPrefixInsideKeySubtree(): void
    {
        putenv('AWS_S3_KEY=some-key');

        self::assertTrue(StaleFixturePrefix::isS3FixtureOfSomeRun('some-key/ci-123/tests-bigquery/file.csv'));
    }

    public function testS3MatcherRejectsRunPrefixOutsideKeySubtree(): void
    {
        putenv('AWS_S3_KEY=some-key');

        self::assertFalse(StaleFixturePrefix::isS3FixtureOfSomeRun('ci-123/file.csv'));
        self::assertFalse(StaleFixturePrefix::isS3FixtureOfSomeRun('other-key/ci-123/file.csv'));
        self::assertFalse(StaleFixturePrefix::isS3FixtureOfSomeRun('some-keyish/ci-123/file.csv'));
    }

    public function testS3MatcherRejectsForeignPathInsideKeySubtree(): void
    {
        putenv('AWS_S3_KEY=some-key');

        self::assertFalse(StaleFixturePrefix::isS3FixtureOfSomeRun('some-key/manual/file.csv'));
    }

    public function testS3MatcherFallsBackToRootWithoutKey(): void
    {
        self::assertTrue(StaleFixturePrefix::isS3FixtureOfSomeRun('ci-123/file.csv'));
        self::assertFalse(StaleFixturePrefix::isS3FixtureOfSomeRun('manual/file.csv'));
    }

    public function testS3MatcherToleratesSlashPaddedKey(): void
    {
        putenv('AWS_S3_KEY=/some-key/');

        self::assertTrue(StaleFixturePrefix::isS3FixtureOfSomeRun('some-key/ci-123/file.csv'));
    }
}
