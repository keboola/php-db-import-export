<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;

final class FailingRangedInsertBigQueryClient extends BigQueryClientWrapper
{
    private int $rangedInsertCount = 0;

    /**
     * @param array<mixed> $config
     */
    public function __construct(array $config, private readonly int $failOnRangedInsert)
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
        if (is_string($sql) && str_starts_with($sql, 'INSERT INTO') && str_contains($sql, ' WHERE ')) {
            $this->rangedInsertCount++;
            if ($this->rangedInsertCount === $this->failOnRangedInsert) {
                throw new ServiceException('Injected failure in a ranged insert.');
            }
        }

        return parent::runQuery($query, $options);
    }
}
