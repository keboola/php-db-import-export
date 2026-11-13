<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

/**
 * How the final-table step of a Snowflake import writes the destination.
 * Only incremental loads with a primary key and full loads differ between the two concrete methods.
 */
enum ImportMethod: string
{
    // resolved by ImportMethodResolver to the project feature or the platform default
    case AUTO = 'auto';
    // one MERGE (incremental) or one INSERT OVERWRITE (full); atomic, one row per key
    case SINGLE_STATEMENT = 'single_statement';
    // dedup table, TRUNCATE/UPDATE/DELETE/INSERT inside an explicit transaction
    case MULTI_STATEMENT = 'multi_statement';
}
