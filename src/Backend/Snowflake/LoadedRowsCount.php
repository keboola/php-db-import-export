<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

/**
 * Row counts out of what a load statement itself answers, so a load does not have to ask
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

    /**
     * Reads the row count off an INSERT result row. Snowflake answers an INSERT with a single row
     * holding the number of inserted rows, named after the statement shape, so the value is taken
     * positionally.
     *
     * @param array<string, mixed>|false $insertResult
     */
    public static function fromInsertResult(array|false $insertResult): int
    {
        if ($insertResult === false) {
            return 0;
        }

        $values = array_values($insertResult);
        if (!array_key_exists(0, $values) || !is_numeric($values[0])) {
            return 0;
        }

        return (int) $values[0];
    }
}
