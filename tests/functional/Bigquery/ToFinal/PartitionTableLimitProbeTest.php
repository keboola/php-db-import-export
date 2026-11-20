<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery\ToFinal;

use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Tests\Keboola\Db\ImportExportFunctional\Bigquery\BigqueryBaseTestCase;
use Throwable;

/**
 * TEMPORARY PROBE, DO NOT MERGE: records what BigQuery answers when a write takes a table over 10,000 partitions.
 */
class PartitionTableLimitProbeTest extends BigqueryBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());
    }

    public function testProbe(): void
    {
        $schema = BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName());
        $insert = static fn(string $table, int $from, int $to): string => sprintf(
            'INSERT INTO %s.`%s` (`id`, `part`) '
                . "SELECT n, DATE_ADD(DATE '2000-01-01', INTERVAL n DAY) FROM UNNEST(GENERATE_ARRAY(%d, %d)) AS n",
            $schema,
            $table,
            $from,
            $to,
        );
        $results = [];
        $run = function (string $label, string $sql) use (&$results): void {
            try {
                $this->bqClient->runQuery($this->bqClient->query($sql));
                $results[] = $label . ' => OK';
            } catch (Throwable $e) {
                $results[] = $label . ' => ' . $e::class . ': ' . $e->getMessage();
            }
        };

        foreach (['probe_a', 'probe_b'] as $table) {
            $this->bqClient->runQuery($this->bqClient->query(sprintf(
                'CREATE TABLE %s.`%s` (`id` INT64, `part` DATE) PARTITION BY `part`',
                $schema,
                $table,
            )));
        }
        $run('setup 0-3999', $insert('probe_a', 0, 3999));
        $run('setup 4000-7999', $insert('probe_a', 4000, 7999));
        $run('setup 8000-9499', $insert('probe_a', 8000, 9499));

        $run('A: INSERT +1000 partitions onto 9500', $insert('probe_a', 9500, 10499));
        $run('B: INSERT +4000 partitions onto 9500', $insert('probe_a', 9500, 13499));
        $run('C: INSERT +5000 partitions onto 9500', $insert('probe_a', 9500, 14499));
        $run('E: MERGE +1000 partitions onto 9500', sprintf(
            'MERGE %s.`probe_a` AS d USING ('
                . "SELECT n AS `id`, DATE_ADD(DATE '2000-01-01', INTERVAL n DAY) AS `part` "
                . 'FROM UNNEST(GENERATE_ARRAY(9500, 10499)) AS n'
                . ') AS s ON d.`id` = s.`id` AND d.`part` = s.`part` '
                . 'WHEN NOT MATCHED THEN INSERT (`id`, `part`) VALUES (s.`id`, s.`part`)',
            $schema,
        ));
        $run('D: transaction of 3 x 4000 partitions into an empty table', sprintf(
            'BEGIN TRANSACTION; %s; %s; %s; COMMIT TRANSACTION;',
            $insert('probe_b', 0, 3999),
            $insert('probe_b', 4000, 7999),
            $insert('probe_b', 8000, 11999),
        ));
        $run('D: rows in probe_b afterwards (job result not read)', sprintf(
            'ASSERT (SELECT COUNT(*) FROM %s.`probe_b`) = 0 AS "probe_b is not empty"',
            $schema,
        ));

        self::fail("PARTITION-TABLE-LIMIT-PROBE-RESULT\n" . implode("\n", $results));
    }
}
