<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Generator;
use InvalidArgumentException;
use Keboola\Db\ImportExport\Backend\Snowflake\ImportMethod;
use Keboola\Db\ImportExport\Backend\Snowflake\ImportMethodResolver;
use Keboola\Db\ImportExport\Backend\Snowflake\ImportMethodSource;
use Keboola\Db\ImportExport\Backend\Snowflake\ResolvedImportMethod;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Decision table of the Snowflake import method: an explicit value wins, the project feature applies
 * only to AUTO, everything else falls back to the platform default.
 */
class ImportMethodResolverTest extends TestCase
{
    private const FEATURE = SnowflakeImportOptions::FEATURE_OPTIMIZED_IMPORT;

    /**
     * @return Generator<string, array{ImportMethod, string[], ImportMethod, ImportMethodSource}>
     */
    public static function decisionTable(): Generator
    {
        yield 'auto without features' => [
            ImportMethod::AUTO,
            [],
            ImportMethod::MULTI_STATEMENT,
            ImportMethodSource::DEFAULT,
        ];
        yield 'auto with unrelated features' => [
            ImportMethod::AUTO,
            ['some-other-feature', 'snowflake-optimized-import-v2'],
            ImportMethod::MULTI_STATEMENT,
            ImportMethodSource::DEFAULT,
        ];
        yield 'auto with the feature' => [
            ImportMethod::AUTO,
            ['some-other-feature', self::FEATURE],
            ImportMethod::SINGLE_STATEMENT,
            ImportMethodSource::PROJECT_FEATURE,
        ];
        yield 'explicit single statement without the feature' => [
            ImportMethod::SINGLE_STATEMENT,
            [],
            ImportMethod::SINGLE_STATEMENT,
            ImportMethodSource::REQUEST,
        ];
        yield 'explicit single statement with the feature' => [
            ImportMethod::SINGLE_STATEMENT,
            [self::FEATURE],
            ImportMethod::SINGLE_STATEMENT,
            ImportMethodSource::REQUEST,
        ];
        yield 'explicit multi statement without the feature' => [
            ImportMethod::MULTI_STATEMENT,
            [],
            ImportMethod::MULTI_STATEMENT,
            ImportMethodSource::REQUEST,
        ];
        yield 'explicit multi statement overrides the feature' => [
            ImportMethod::MULTI_STATEMENT,
            [self::FEATURE],
            ImportMethod::MULTI_STATEMENT,
            ImportMethodSource::REQUEST,
        ];
    }

    /**
     * @param string[] $features
     */
    #[DataProvider('decisionTable')]
    public function testResolve(
        ImportMethod $requested,
        array $features,
        ImportMethod $expectedMethod,
        ImportMethodSource $expectedSource,
    ): void {
        $resolved = ImportMethodResolver::resolve($requested, $features);

        self::assertSame($expectedMethod, $resolved->method);
        self::assertSame($expectedSource, $resolved->source);
        self::assertSame($expectedMethod === ImportMethod::SINGLE_STATEMENT, $resolved->isSingleStatement());
    }

    /**
     * @return Generator<string, array{string[], ImportMethod, ImportMethodSource}>
     */
    public static function featureListOnly(): Generator
    {
        yield 'no features' => [[], ImportMethod::MULTI_STATEMENT, ImportMethodSource::DEFAULT];
        yield 'unrelated feature' => [
            ['some-other-feature'],
            ImportMethod::MULTI_STATEMENT,
            ImportMethodSource::DEFAULT,
        ];
        yield 'the feature' => [
            [self::FEATURE],
            ImportMethod::SINGLE_STATEMENT,
            ImportMethodSource::PROJECT_FEATURE,
        ];
    }

    /**
     * Callers of the split package that pass only the feature list must keep the method they got before.
     *
     * @param string[] $features
     */
    #[DataProvider('featureListOnly')]
    public function testOptionsWithoutResolvedMethodResolveAutoAgainstTheirFeatures(
        array $features,
        ImportMethod $expectedMethod,
        ImportMethodSource $expectedSource,
    ): void {
        $options = new SnowflakeImportOptions(features: $features);

        self::assertSame($expectedMethod, $options->getImportMethod()->method);
        self::assertSame($expectedSource, $options->getImportMethod()->source);
        self::assertSame($expectedMethod === ImportMethod::SINGLE_STATEMENT, $options->useOptimizedImport());
    }

    public function testResolvedMethodRejectsAuto(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new ResolvedImportMethod(ImportMethod::AUTO, ImportMethodSource::REQUEST);
    }
}
