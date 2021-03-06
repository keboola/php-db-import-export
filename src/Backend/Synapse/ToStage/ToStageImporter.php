<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use LogicException;

final class ToStageImporter implements ToStageImporterInterface
{
    private const TIMER_TABLE_IMPORT = 'copyToStaging';

    /** @var Connection */
    private $connection;

    /** @var CopyAdapterInterface|null */
    private $adapter;

    public function __construct(
        Connection $connection,
        ?CopyAdapterInterface $adapter = null
    ) {
        $this->connection = $connection;
        $this->adapter = $adapter;
    }

    public function importToStagingTable(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destinationDefinition,
        ImportOptionsInterface $options
    ): ImportState {
        assert($options instanceof SynapseImportOptions);
        assert($destinationDefinition instanceof SynapseTableDefinition);
        Assert::assertValidSource($source);
        Assert::assertColumnsOnTableDefinition($source, $destinationDefinition);
        $state = new ImportState($destinationDefinition->getTableName());

        $adapter = $this->getAdapter($source);

        $state->startTimer(self::TIMER_TABLE_IMPORT);
        try {
            $state->addImportedRowsCount(
                $adapter->runCopyCommand(
                    $source,
                    $destinationDefinition,
                    $options
                )
            );
        } catch (\Doctrine\DBAL\Exception $e) {
            throw SynapseException::covertException($e);
        }
        $state->stopTimer(self::TIMER_TABLE_IMPORT);

        return $state;
    }

    private function getAdapter(Storage\SourceInterface $source): CopyAdapterInterface
    {
        if ($this->adapter !== null) {
            return $this->adapter;
        }

        switch (true) {
            case $source instanceof Storage\ABS\SourceFile:
                $adapter = new FromABSCopyIntoAdapter($this->connection);
                break;
            case $source instanceof Storage\SqlSourceInterface:
                $adapter = new FromTableInsertIntoAdapter($this->connection);
                break;
            default:
                throw new LogicException(
                    sprintf(
                        'No suitable adapter found for source: "%s".',
                        get_class($source)
                    )
                );
        }
        return $adapter;
    }
}
