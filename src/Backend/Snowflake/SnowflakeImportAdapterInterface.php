<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Generator;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\SnowflakeDbAdapter\Connection;

interface SnowflakeImportAdapterInterface extends BackendImportAdapterInterface
{
    /**
     * Snowflake import is handled differently for copy table2table and file2table
     *
     * @return int - number of imported rows
     */
    public function executeCopyCommands(
        Generator $commands,
        Connection $connection,
        DestinationInterface $destination,
        ImportOptions $importOptions,
        ImportState $importState
    ): int;
}
