<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;
use Keboola\Db\ImportExport\Storage;

class BigqueryBaseTestCase extends ImportExportBaseTest
{
    public const TESTS_PREFIX = 'ieLibTest_';
    public const TEST_DATABASE = self::TESTS_PREFIX . 'refTableDatabase';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    protected const BIGQUERY_SOURCE_DATABASE_NAME = 'tests_source';
    protected const BIGQUERY_DESTINATION_DATABASE_NAME = 'tests_destination';
    public const TABLE_TRANSLATIONS = 'transactions';

    public const TABLE_TABLE = 'test_table';

    protected BigQueryClient $bqClient;

    protected function setUp(): void
    {
        parent::setUp();
        $this->bqClient = $this->getTeradataConnection();
    }

    protected function cleanDatabase(string $dbname): void
    {
        if (!$this->datasetExists($dbname)) {
            return;
        }

        $this->bqClient->dataset($dbname)->delete(['deleteContents' => true]);
    }

    protected function createDatabase(string $dbName): void
    {
        $this->bqClient->createDataset($dbName);
    }

    private function getTeradataConnection(): BigQueryClient
    {
        $keyFile = getenv('BQ_KEY_FILE');
        if ($keyFile === false) {
            throw new LogicException('Env "BQ_KEY_FILE" is empty');
        }

        /** @var array<string, string> $credentials */
        $credentials = json_decode($keyFile, true, 512, JSON_THROW_ON_ERROR);
        assert($credentials !== false);
        return new BigQueryClient([
            'keyFile' => $credentials,
        ]);
    }

    protected function datasetExists(string $datasetName): bool
    {
        return $this->bqClient->dataset($datasetName)->exists();
    }

    /**
     * @param string[] $convertEmptyValuesToNull
     */
    protected function getImportOptions(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0
    ): ImportOptions {
        return
            new ImportOptions(
                $convertEmptyValuesToNull,
                $isIncremental,
                $useTimestamp,
                $numberOfIgnoredLines,
            );
    }

    protected function getSourceDbName(): string
    {
        return self::BIGQUERY_SOURCE_DATABASE_NAME
            . '_'
            . getenv('SUITE');
    }

    protected function getDestinationDbName(): string
    {
        return self::BIGQUERY_DESTINATION_DATABASE_NAME
            . '_'
            . getenv('SUITE');
    }

    protected function initTable(string $tableName, string $dbName = ''): void
    {
        if ($dbName === '') {
            $dbName = $this->getDestinationDbName();
        }

        switch ($tableName) {
            case self::TABLE_TRANSLATIONS:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s
            (
              `id` INT64 ,
              `name` STRING(50),
              `price` DECIMAL,
              `isDeleted` INT64
           )',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->bqClient->runQuery($this->bqClient->query(
                    sprintf(
                        'CREATE TABLE %s.%s (
          `col1` STRING(500),
          `col2` STRING(500),
          `_timestamp` TIMESTAMP
        );',
                        BigqueryQuote::quoteSingleIdentifier($dbName),
                        BigqueryQuote::quoteSingleIdentifier($tableName)
                    )
                ));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', CURRENT_TIMESTAMP());',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
          `col1` STRING(50),
          `col2` STRING(50) 
        );',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'a\', \'b\');',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'c\', \'d\');',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_LEMMA:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
          `ts` STRING(50),
          `lemma` STRING(50),
          `lemmaIndex` STRING(50),
                `_timestamp` TIMESTAMP
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                `id` STRING(50),
                `idTwitter` STRING(50),
                `name` STRING(100),
                `import` STRING(50),
                `isImported` STRING(50),
                `apiLimitExceededDatetime` STRING(50),
                `analyzeSentiment` STRING(50),
                `importKloutScore` STRING(50),
                `timestamp` STRING(50),
                `oauthToken` STRING(50),
                `oauthSecret` STRING(50),
                `idApp` STRING(50),
                `_timestamp` TIMESTAMP
            )',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_TABLE:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                                `column` STRING(50)         ,
                                `table` STRING(50)      ,
                                `lemmaIndex` STRING(50),
                `_timestamp` TIMESTAMP
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                                `col1` STRING(50)         ,
                                `col2` STRING(50)      
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            default:
                throw new Exception('unknown table');
        }
    }

    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ImportOptions {
        return new ImportOptions(
            [],
            false,
            true,
            $skipLines
        );
    }

    protected function initSingleTable(
        string $db = self::BIGQUERY_SOURCE_DATABASE_NAME,
        string $table = self::TABLE_TABLE
    ): void {
        if (!$this->datasetExists($db)) {
            $this->createDatabase($db);
        }
        // char because of Stats test
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s
            (
`Other`     STRING(50)
    );',
                BigqueryQuote::quoteSingleIdentifier($db),
                BigqueryQuote::quoteSingleIdentifier($table)
            )
        ));
    }

    /**
     * filePath is expected without AWS_GCS_KEY
     *
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     */
    protected function createGCSSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ): Storage\GCS\SourceFile {
        if ($isDirectory) {
            throw new Exception('Directory not supported for GCS');
        }

        return new Storage\GCS\SourceFile(
            (string) getenv('BQ_BUCKET_NAME'),
            $filePath,
            '',
            $this->getGCSCredentials(),
            $options,
            $isSliced,
            $columns,
            $primaryKeys
        );
    }
}
