<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;

final class FailingRangedMergeBigQueryClient extends BigQueryClientWrapper
{
    private int $rangedMergeCount = 0;

    /**
     * @param array<mixed> $config
     */
    public function __construct(array $config, private readonly int $failOnRangedMerge)
    {
        parent::__construct($config, 'e2e-ie-lib');
    }

    /**
     * @param array<mixed> $options
     */
    public function runQuery(JobConfigurationInterface $query, array $options = []): QueryResults
    {
        $configuration = $query->toArray()['configuration'] ?? null;
        $queryConfiguration = is_array($configuration) ? ($configuration['query'] ?? null) : null;
        $sql = is_array($queryConfiguration) ? ($queryConfiguration['query'] ?? null) : null;
        if (is_string($sql) && str_starts_with($sql, 'MERGE ') && str_contains($sql, ' USING (SELECT ')) {
            $this->rangedMergeCount++;
            if ($this->rangedMergeCount === $this->failOnRangedMerge) {
                throw new ServiceException('Injected failure in a ranged merge.');
            }
        }

        return parent::runQuery($query, $options);
    }
}
