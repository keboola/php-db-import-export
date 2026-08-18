<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\ImportExport\Exception\ImportExportException;

/**
 * Row counts out of what `COPY INTO <table>` itself answers, so a load does not have to ask
 * INFORMATION_SCHEMA how many rows it produced.
 */
final class LoadedRowsCount
{
    /**
     * Sums ROWS_LOADED over the per-file rows of one `COPY INTO <table>` result set. Snowflake
     * answers with a single status row carrying no ROWS_LOADED when it processes no file at all,
     * which counts as zero loaded rows. Any other row without a usable ROWS_LOADED throws: the
     * count lands in the job result and the import event, so a result set this class can no
     * longer read must not pass as zero rows.
     *
     * @param array<array<string, mixed>> $copyResult
     * @throws ImportExportException
     */
    public static function fromCopyIntoResult(array $copyResult): int
    {
        $rowsLoaded = 0;
        foreach ($copyResult as $row) {
            // Column casing is not part of the COPY INTO contract.
            $row = array_change_key_case($row);

            $fileRows = $row['rows_loaded'] ?? null;
            if (is_numeric($fileRows)) {
                $rowsLoaded += (int) $fileRows;
                continue;
            }

            $isNoFileProcessedRow = count($copyResult) === 1
                && array_key_exists('status', $row)
                && !array_key_exists('file', $row);
            if ($isNoFileProcessedRow) {
                return 0;
            }

            throw new ImportExportException(
                sprintf(
                    'Snowflake COPY INTO result row carries no usable "rows_loaded", columns: "%s".',
                    implode('", "', array_keys($row)),
                ),
                ImportExportException::UNKNOWN_ERROR,
            );
        }

        return $rowsLoaded;
    }
}
