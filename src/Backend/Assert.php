<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Datatype\Definition\Common;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Utils\CaseConverter;

class Assert
{
    /**
     * @throws ColumnsMismatchException
     */
    public static function assertSameColumns(
        ColumnCollection $source,
        ColumnCollection $destination
    ): void {
        $it0 = $source->getIterator();
        $it1 = $destination->getIterator();
        while ($it0->valid() || $it1->valid()) {
            if ($it0->valid() && $it1->valid()) {
                /** @var ColumnInterface $sourceCol */
                $sourceCol = $it0->current();
                /** @var ColumnInterface $destCol */
                $destCol = $it1->current();
                if ($sourceCol->getColumnName() !== $destCol->getColumnName()) {
                    throw ColumnsMismatchException::createColumnsNamesMismatch($sourceCol, $destCol);
                }
                /** @var Common $sourceDef */
                $sourceDef = $sourceCol->getColumnDefinition();
                /** @var Common $destDef */
                $destDef = $destCol->getColumnDefinition();

                if ($sourceDef->getType() !== $destDef->getType()) {
                    throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
                }

                $isLengthMismatch = $sourceDef->getLength() !== $destDef->getLength();

                if ($isLengthMismatch) {
                    throw ColumnsMismatchException::createColumnsMismatch($sourceCol, $destCol);
                }
            } else {
                throw ColumnsMismatchException::createColumnsCountMismatch($source, $destination);
            }

            $it0->next();
            $it1->next();
        }
    }

    public static function assertColumnsOnTableDefinition(
        SourceInterface $source,
        TableDefinitionInterface $destinationDefinition
    ): void {
        if (count($source->getColumnsNames()) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $moreColumns = array_diff($source->getColumnsNames(), $destinationDefinition->getColumnsNames());
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }

    public static function assertColumnsOnTableDefinitionCaseInsensitive(
        SourceInterface $source,
        TableDefinitionInterface $destinationDefinition
    ): void {
        $columnsNames = CaseConverter::arrayToUpper($source->getColumnsNames());
        if (count($columnsNames) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $destinationColumnsNames = CaseConverter::arrayToUpper($destinationDefinition->getColumnsNames());
        $moreColumns = array_diff($columnsNames, $destinationColumnsNames);
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }
}
