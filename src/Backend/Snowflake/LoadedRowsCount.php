<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

/**
 * Row counts out of what `COPY INTO <table>` itself answers, so a load does not have to ask
 * INFORMATION_SCHEMA how many rows it produced.
 */
final class LoadedRowsCount
{
    /**
     * Sums ROWS_LOADED over the per-file rows of one `COPY INTO <table>` result set. Snowflake
     * answers with a single status row carrying no ROWS_LOADED when it processes no file at all,
     * which counts as zero loaded rows.
     *
     * @param array<array<string, mixed>> $copyResult
     */
    public static function fromCopyIntoResult(array $copyResult): int
    {
        $rowsLoaded = 0;
        foreach ($copyResult as $row) {
            $fileRows = $row['rows_loaded'] ?? 0;
            if (is_numeric($fileRows)) {
                $rowsLoaded += (int) $fileRows;
            }
        }

        return $rowsLoaded;
    }
}
