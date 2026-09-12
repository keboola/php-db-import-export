<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

use RuntimeException;

/**
 * Run-unique path prefix for test fixtures in the shared CI file storages.
 *
 * One bucket/container is shared by every branch and every suite, so fixtures
 * live under BUILD_PREFIX/SUITE the same way test schemas and datasets carry
 * BUILD_PREFIX. Without it a run that clears its fixtures deletes the files a
 * concurrent run is importing from, and the Snowflake+GCS and BigQuery suites
 * collide even inside a single run whenever both point at the same bucket.
 *
 * Both halves are optional so a local run without them keeps fixtures at the
 * storage root.
 */
final class FixturePath
{
    private const SAFE_PART_PATTERN = '~^[A-Za-z0-9._-]+$~';

    public static function prefix(): string
    {
        $segments = array_filter([
            self::segment('BUILD_PREFIX'),
            self::segment('SUITE'),
        ], static fn(string $segment): bool => $segment !== '');

        return $segments === [] ? '' : implode('/', $segments) . '/';
    }

    public static function in(string $path = ''): string
    {
        return self::prefix() . ltrim($path, '/');
    }

    /**
     * Fixture path including AWS_S3_KEY, the bucket sub-path the S3 fixtures
     * have always lived under.
     */
    public static function inS3(string $path = ''): string
    {
        $key = self::segment('AWS_S3_KEY');

        return ($key === '' ? '' : $key . '/') . self::in($path);
    }

    /**
     * Prefix that scopes a destructive operation. It must be narrowed by
     * BUILD_PREFIX or SUITE - a storage-specific segment such as AWS_S3_KEY is
     * shared by every run and would let the caller clear the whole storage.
     */
    public static function requireScope(string $scope): string
    {
        if (self::prefix() === '') {
            throw new RuntimeException(
                'Refusing to clear an unscoped storage. Set BUILD_PREFIX or SUITE to isolate this run.',
            );
        }

        return $scope;
    }

    /**
     * The prefix is interpolated into a `gsutil rm` wildcard, so a glob
     * character in it would clear other runs' fixtures.
     */
    private static function segment(string $envName): string
    {
        $value = trim((string) getenv($envName), " \t\n\r\0\x0B/");
        if ($value === '') {
            return '';
        }

        foreach (explode('/', $value) as $part) {
            if ($part === '.' || $part === '..' || preg_match(self::SAFE_PART_PATTERN, $part) !== 1) {
                throw new RuntimeException(sprintf(
                    '%s must match %s in every path part, "%s" given.',
                    $envName,
                    self::SAFE_PART_PATTERN,
                    $value,
                ));
            }
        }

        return $value;
    }
}
