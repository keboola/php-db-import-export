<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon\Cleanup;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

/**
 * Drops stale test schemas left behind by previous CI runs.
 *
 * Test schemas carry a run-unique BUILD_PREFIX (see the *SchemaName getters),
 * so cancelled runs (cancel-in-progress) whose tearDown never fired orphan
 * their schemas. We cannot enumerate other runs' ids, so we delete by AGE:
 * any test schema created longer ago than the TTL cannot belong to a live run
 * (a running job's schemas are minutes old). The age comparison is done in
 * Snowflake against CURRENT_TIMESTAMP(), so it is timezone-agnostic.
 */
final class SnowflakeCleaner
{
    /**
     * Base names of every schema these tests create (before the -<buildPrefix>-<suite> suffix).
     * A schema is only ever dropped if its name starts with one of these AND it is stale.
     */
    private const TEST_SCHEMA_PREFIXES = [
        'in_c_tests',
        'some_tests',
        'in.c-tests',
        'some.tests',
        'import_export_test',
    ];

    public function __construct(private readonly Connection $connection)
    {
    }

    public static function fromEnv(): self
    {
        $connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            ],
        );

        return new self($connection);
    }

    public function cleanOlderThan(int $ttlSeconds): void
    {
        // Filter by age in-engine so it is immune to session-timezone differences.
        $staleSchemas = $this->connection->fetchFirstColumn(sprintf(
            'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA '
            . 'WHERE CREATED < DATEADD(second, %d, CURRENT_TIMESTAMP())',
            -$ttlSeconds,
        ));

        $dropped = 0;
        foreach ($staleSchemas as $schemaName) {
            if (!is_string($schemaName) || !self::isTestSchema($schemaName)) {
                continue;
            }

            try {
                $this->connection->executeStatement(sprintf(
                    'DROP SCHEMA IF EXISTS %s CASCADE',
                    SnowflakeQuote::quoteSingleIdentifier($schemaName),
                ));
                $dropped++;
                printf("[cleanup:snowflake] dropped stale schema %s\n", $schemaName);
            } catch (Throwable $e) {
                // Best effort - a schema another run is dropping concurrently is fine to skip.
                printf("[cleanup:snowflake] skip %s: %s\n", $schemaName, $e->getMessage());
            }
        }

        printf("[cleanup:snowflake] done, dropped %d stale schema(s)\n", $dropped);
        $this->connection->close();
    }

    private static function isTestSchema(string $name): bool
    {
        foreach (self::TEST_SCHEMA_PREFIXES as $prefix) {
            if (str_starts_with($name, $prefix)) {
                return true;
            }
        }

        return false;
    }
}
