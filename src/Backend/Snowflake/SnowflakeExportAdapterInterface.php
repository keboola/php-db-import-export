<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;

/**
 * @deprecated use BackendExportAdapterInterface
 */
interface SnowflakeExportAdapterInterface extends BackendExportAdapterInterface
{
    public function __construct(Connection $connection);
}
