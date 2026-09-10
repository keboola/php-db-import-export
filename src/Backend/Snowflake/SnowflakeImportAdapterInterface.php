<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;

interface SnowflakeImportAdapterInterface extends BackendImportAdapterInterface
{
    public function __construct(Connection $connection);
}
