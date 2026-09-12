<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Common;

use PHPUnit\Framework\Attributes\DataProvider;
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

    /**
     * @return iterable<string, array{0: string}>
     */
    public static function unsafeSegmentProvider(): iterable
    {
        yield 'glob star' => ['ci-*'];
        yield 'glob question mark' => ['ci-?23'];
        yield 'glob range' => ['ci-[0-9]'];
        yield 'parent traversal' => ['ci-123/..'];
        yield 'space' => ['ci 123'];
        yield 'shell metacharacter' => ['ci-123;rm'];
    }

    #[DataProvider('unsafeSegmentProvider')]
    public function testRejectsUnsafeBuildPrefix(string $value): void
    {
        putenv(sprintf('BUILD_PREFIX=%s', $value));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('BUILD_PREFIX must match');
        FixturePath::prefix();
    }

    #[DataProvider('unsafeSegmentProvider')]
    public function testRejectsUnsafeSuite(string $value): void
    {
        putenv(sprintf('SUITE=%s', $value));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SUITE must match');
        FixturePath::prefix();
    }

    public function testAcceptsNestedKeySegments(): void
    {
        putenv('AWS_S3_KEY=nested/key/path');
        putenv('BUILD_PREFIX=ci-123');

        self::assertSame('nested/key/path/ci-123/x.csv', FixturePath::inS3('x.csv'));
    }
}
