<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Keboola\Db\ImportExportCommon\FixturePath;

class FixturePathTest extends TestCase
{
    /** @var array<string, string|false> */
    private array $originalEnv = [];

    protected function setUp(): void
    {
        parent::setUp();
        foreach (['BUILD_PREFIX', 'SUITE', 'AWS_S3_KEY'] as $name) {
            $this->originalEnv[$name] = getenv($name);
            putenv($name);
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->originalEnv as $name => $value) {
            if ($value === false) {
                putenv($name);
            } else {
                putenv(sprintf('%s=%s', $name, $value));
            }
        }
        parent::tearDown();
    }

    public function testPrefixCombinesBuildPrefixAndSuite(): void
    {
        putenv('BUILD_PREFIX=ci-123');
        putenv('SUITE=tests-bigquery');

        self::assertSame('ci-123/tests-bigquery/', FixturePath::prefix());
        self::assertSame('ci-123/tests-bigquery/sliced/file.csv', FixturePath::in('/sliced/file.csv'));
    }

    public function testPrefixIsEmptyWithoutEnv(): void
    {
        self::assertSame('', FixturePath::prefix());
        self::assertSame('file.csv', FixturePath::in('file.csv'));
    }

    public function testInS3PrependsKey(): void
    {
        putenv('AWS_S3_KEY=some-key');
        putenv('BUILD_PREFIX=ci-123');

        self::assertSame('some-key/ci-123/file.csv', FixturePath::inS3('file.csv'));
    }

    public function testRequireScopePassesWithRunPrefix(): void
    {
        putenv('BUILD_PREFIX=ci-123');

        self::assertSame('ci-123/', FixturePath::requireScope(FixturePath::in()));
    }

    public function testRequireScopeAcceptsSuiteOnlyScope(): void
    {
        // The composer load scripts set SUITE only; BUILD_PREFIX is a CI addition.
        putenv('SUITE=tests-snowflake');

        self::assertSame('tests-snowflake/', FixturePath::requireScope(FixturePath::in()));
    }

    public function testRequireScopeRefusesKeyOnlyScope(): void
    {
        putenv('AWS_S3_KEY=some-key');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Refusing to clear an unscoped storage.');
        FixturePath::requireScope(FixturePath::inS3());
    }

    public function testRequireScopeRefusesEmptyScope(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Refusing to clear an unscoped storage.');
        FixturePath::requireScope(FixturePath::in());
    }
}
