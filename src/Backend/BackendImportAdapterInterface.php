<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Generator;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;

interface BackendImportAdapterInterface
{
    public function __construct(SourceInterface $source);

    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): Generator;
}
