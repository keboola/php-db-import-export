<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Exception;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;

class ColumnsMismatchException extends ImportExportException
{
    /**
     * @param string[] $sourcePrimaryKeys
     * @param string[] $destinationPrimaryKeys
     */
    public static function createPrimaryKeysColumnsMismatch(
        array $sourcePrimaryKeys,
        array $destinationPrimaryKeys,
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Primary keys do not match between source and destination tables. Source: "%s", Destination: "%s"',
            implode(',', $sourcePrimaryKeys),
            implode(',', $destinationPrimaryKeys),
        ));
    }
    public static function createColumnsNamesMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef,
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Source destination columns name mismatch. "%s"->"%s"',
            $sourceDef->getColumnName(),
            $destDef->getColumnName(),
        ));
    }

    public static function createColumnsMismatch(
        ColumnInterface $sourceDef,
        ColumnInterface $destDef,
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Source destination columns mismatch. "%s %s"->"%s %s"',
            $sourceDef->getColumnName(),
            $sourceDef->getColumnDefinition()->getSQLDefinition(),
            $destDef->getColumnName(),
            $destDef->getColumnDefinition()->getSQLDefinition(),
        ));
    }

    public static function createColumnByNameMissing(
        ColumnInterface $sourceDef,
    ): ColumnsMismatchException {
        return new self(sprintf(
            'Source column "%s" not found in destination table',
            $sourceDef->getColumnName(),
        ));
    }

    public static function createColumnsCountMismatch(
        ColumnCollection $source,
        ColumnCollection $destination,
    ): ColumnsMismatchException {
        $columnsSource = array_map(
            static fn(ColumnInterface $col) => $col->getColumnName(),
            iterator_to_array($source->getIterator()),
        );
        $columnsDestination = array_map(
            static fn(ColumnInterface $col) => $col->getColumnName(),
            iterator_to_array($destination->getIterator()),
        );
        return new self(
            sprintf(
                'Tables don\'t have same number of columns. Source columns: "%s", Destination columns: "%s"',
                implode(',', $columnsSource),
                implode(',', $columnsDestination),
            ),
        );
    }
}
