<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery;

use Generator;
use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryResourcesExceededException;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\PartitionRangeColumn;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\PartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\RangePartitioningConfig;
use Keboola\TableBackendUtils\Table\Bigquery\TimePartitioningConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

class BigqueryExceptionTest extends TestCase
{
    // phpcs:ignore Generic.Files.LineLength
    private const RESOURCES_EXCEEDED_BIGQUERY_MESSAGE = 'Resources exceeded during query execution: Your project or organization exceeded the maximum disk and memory limit available for shuffle operations. Consider provisioning more slots, reducing query concurrency, or using more efficient logic in this job.';

    // the Keboola-side levers appended after the BigQuery text
    private const ADVICE = 'Reduce the loaded volume, apply retention on the destination table, '
        . 'or use an append-only load.';

    private const TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE = 'Too many partitions produced by query, allowed 4000, '
        . 'query produces at least 4001 partitions';

    private const TOO_MANY_PARTITIONS_ADVICE = 'A single load touches too many partitions of the destination '
        . 'table. Use coarser partitioning (e.g. by month), a different partition column, or split the load.';

    // phpcs:ignore Generic.Files.LineLength
    private const TABLE_PARTITION_LIMIT_BIGQUERY_MESSAGE = 'Resources exceeded during query execution: Table my-project:out_c_main.dest will have 10500 partitions when the job finishes, exceeding limit 10000. If partitions were recently expired, it may take some time to be reflected unless explicitly deleted.';

    /**
     * @param mixed[]            $job
     * @param callable(Throwable $throwable): void $expectedThrowableAssertion
     */
    #[DataProvider('provideJobAndExpectedError')]
    public function testCreateExceptionFromJobResult(array $job, callable $expectedThrowableAssertion): void
    {
        $e = BigqueryException::createExceptionFromJobResult($job);
        $expectedThrowableAssertion($e);
    }

    public static function provideJobAndExpectedError(): Generator
    {
        yield 'single error' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 1; errors: 1; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 1; errors: 1; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                self::assertStringStartsWith(
                    'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp.'
                    . ' Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]];'
                    . ' line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:',
                    $e->getMessage(),
                );
                self::assertCount(1, explode(PHP_EOL, $e->getMessage()));
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
            },
        ];
        yield 'multiple errors' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2; errors: 2; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2; errors: 2; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                // phpcs:ignore Generic.Files.LineLength
                self::assertStringStartsWith(
                    'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. '
                    . 'Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; '
                    . 'line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:',
                    $e->getMessage(),
                );
                // phpcs:ignore Generic.Files.LineLength
                self::assertStringContainsString(
                // different line number
                    'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp.'
                    . ' Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]];'
                    . ' line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 '
                    . 'column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File:',
                    $e->getMessage(),
                );
                self::assertCount(2, explode(PHP_EOL, $e->getMessage()));
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
            },
        ];
        yield 'too many errors' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 2 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],

                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                self::assertStringStartsWith(
                    'There were too many errors during the import. For more information check job',
                    $e->getMessage(),
                );

                self::assertCount(1, explode(PHP_EOL, $e->getMessage()));
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
            },
        ];
        yield 'Required column value is missing' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'Required column value is missing',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],

                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                self::assertEquals('Required column value is missing', $e->getMessage());
                self::assertCount(1, explode(PHP_EOL, $e->getMessage()));
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
            },
        ];
        yield 'Some other errors apart from parsing ones' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'I had to output the private key here OLOL',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: Could not parse \'00:00:00\' as a timestamp. Required format is YYYY-MM-DD HH:MM[:SS[.SSSSSS]] or YYYY/MM/DD HH:MM[:SS[.SSSSSS]]; line_number: 3 byte_offset_to_start_of_line: 17 column_index: 1 column_name: "timestamp" column_type: TIMESTAMP value: "00:00:00" File: gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],

                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                self::assertStringContainsString(
                    'There were additional errors during the import. For more information check job ',
                    $e->getMessage(),
                );
                self::assertCount(2, explode(PHP_EOL, $e->getMessage()));
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
            },
        ];
        yield 'No user errors, only application errors' => [
            [
                'kind' => 'bigquery#job',
                'etag' => '/ty8yus/A/JoTZZvf1qgFQ==',
                'id' => 'tf2-56:US.eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                // phpcs:ignore Generic.Files.LineLength
                'selfLink' => 'https://bigquery.googleapis.com/bigquery/v2/projects/tf2-56/jobs/eb64d133-213d-4e99-9cc0-4e37d0c18de9?location=US',
                'user_email' => 'tf2-56@617348738050.iam.gserviceaccount.com',
                'configuration' => [
                    'load' => [
                        'sourceUris' => [
                            0 => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                        ],
                        'schema' => [
                            'fields' => [
                                [
                                    'name' => 'id',
                                    'type' => 'INTEGER',
                                ],
                                [
                                    'name' => 'timestamp',
                                    'type' => 'TIMESTAMP',
                                ],
                            ],
                        ],
                        'destinationTable' => [
                            'projectId' => 'tf2-56',
                            'datasetId' => 'in_c_API_tests_58bf9bd5cc965e125d1c6abf53b6d291ab5c34e8',
                            'tableId' => '__temp_csvimport655f304cc39c67_90384747',
                        ],
                        'fieldDelimiter' => ',',
                        'skipLeadingRows' => 1,
                        'quote' => '"',
                        'allowQuotedNewlines' => true,
                        'sourceFormat' => 'CSV',
                        'autodetect' => false,
                        'preserveAsciiControlCharacters' => true,
                    ],
                    'labels' => [
                        'run_id' => '3118',
                    ],
                    'jobType' => 'LOAD',
                ],
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'statistics' => [
                    'creationTime' => '1700737120686',
                    'startTime' => '1700737121028',
                    'endTime' => '1700737123803',
                    'completionRatio' => 0,
                    'totalSlotMs' => '2172',
                    'reservation_id' => 'default-pipeline',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalid',
                        // phpcs:ignore Generic.Files.LineLength
                        'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalid',
                            // phpcs:ignore Generic.Files.LineLength
                            'message' => 'Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'I had to output the private key here OLOL',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'I had to output the private key here OLOL',
                        ],
                        [
                            'reason' => 'invalid',
                            'location' => 'gs://kbc-tf2-files-storage/exp-15/56/files/2023/11/23/3121.keboola7ecguy.gz',
                            'message' => 'I had to output the private key here OLOL',
                        ],
                    ],
                    'state' => 'DONE',
                ],
                'principal_subject' => 'serviceAccount:tf2-56@617348738050.iam.gserviceaccount.com',
                'jobCreationReason' => [
                    'code' => 'REQUESTED',
                ],
            ],
            static function (Throwable $e) {
                self::assertStringContainsString(
                    'Error while reading data, error message: CSV processing encountered too many errors, '
                    . 'giving up. Rows: 2000; errors: 1500; max bad: 0; error percent: 0',
                    $e->getMessage(),
                );
                self::assertInstanceOf(BigqueryException::class, $e);
            },
        ];

        yield 'resources exceeded' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'resourcesExceeded',
                        'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                    ],
                    'errors' => [
                        [
                            'reason' => 'resourcesExceeded',
                            'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertInstanceOf(BigqueryResourcesExceededException::class, $e);
                // the user-error mapping in the storage driver hangs on this parent
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                // BigQuery text first, then our levers, then the job handle for support
                self::assertSame(
                    self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE . ' ' . self::ADVICE
                    . ' For more information check job "eb64d133-213d-4e99-9cc0-4e37d0c18de9" '
                    . 'in Google Cloud Console.',
                    $e->getMessage(),
                );
            },
        ];

        yield 'resources exceeded does not shadow a later required column error' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'resourcesExceeded',
                        'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                    ],
                    'errors' => [
                        [
                            'reason' => 'resourcesExceeded',
                            'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                        ],
                        [
                            'reason' => 'invalid',
                            'message' => 'Required column value is missing: quantity',
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                self::assertSame('Required column value is missing: quantity', $e->getMessage());
            },
        ];

        yield 'resources exceeded does not shadow a later parse error' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'resourcesExceeded',
                        'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                    ],
                    'errors' => [
                        [
                            'reason' => 'resourcesExceeded',
                            'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                        ],
                        [
                            'reason' => 'invalid',
                            'message' => 'Could not parse \'x\' as INT64 for field quantity',
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                self::assertStringContainsString('Could not parse', $e->getMessage());
            },
        ];

        yield 'error entry without reason key is not a resources exceeded error' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'message' => 'Query error: something else went wrong',
                    ],
                    'errors' => [
                        [
                            'message' => 'Query error: something else went wrong',
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertNotInstanceOf(BigqueryInputDataException::class, $e);
                self::assertInstanceOf(BigqueryException::class, $e);
                self::assertSame('Query error: something else went wrong', $e->getMessage());
            },
        ];

        yield 'too many partitions only in errorResult' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalidQuery',
                        'message' => self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE,
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                self::assertStringStartsWith(
                    self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE . ' ' . self::TOO_MANY_PARTITIONS_ADVICE,
                    $e->getMessage(),
                );
            },
        ];

        yield 'too many partitions' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'invalidQuery',
                        'message' => self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE,
                    ],
                    'errors' => [
                        [
                            'reason' => 'invalidQuery',
                            'message' => self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE,
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
                self::assertSame(
                    self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE . ' ' . self::TOO_MANY_PARTITIONS_ADVICE
                    . ' For more information check job "eb64d133-213d-4e99-9cc0-4e37d0c18de9" '
                    . 'in Google Cloud Console.',
                    $e->getMessage(),
                );
            },
        ];

        yield 'table partition limit' => [
            [
                'jobReference' => [
                    'projectId' => 'tf2-56',
                    'jobId' => 'eb64d133-213d-4e99-9cc0-4e37d0c18de9',
                    'location' => 'US',
                ],
                'status' => [
                    'errorResult' => [
                        'reason' => 'resourcesExceeded',
                        'message' => self::TABLE_PARTITION_LIMIT_BIGQUERY_MESSAGE,
                    ],
                    'errors' => [
                        [
                            'reason' => 'resourcesExceeded',
                            'message' => self::TABLE_PARTITION_LIMIT_BIGQUERY_MESSAGE,
                        ],
                    ],
                    'state' => 'DONE',
                ],
            ],
            static function (Throwable $e) {
                self::assertInstanceOf(BigqueryInputDataException::class, $e);
                self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
                self::assertStringContainsString('will have 10500 partitions', $e->getMessage());
                self::assertStringContainsString('Use coarser partitioning', $e->getMessage());
                self::assertStringContainsString(
                    'check job "eb64d133-213d-4e99-9cc0-4e37d0c18de9"',
                    $e->getMessage(),
                );
            },
        ];
    }

    public function testCovertExceptionTablePartitionLimitIsUserError(): void
    {
        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => self::TABLE_PARTITION_LIMIT_BIGQUERY_MESSAGE,
                    'errors' => [
                        [
                            'message' => self::TABLE_PARTITION_LIMIT_BIGQUERY_MESSAGE,
                            'domain' => 'global',
                            'reason' => 'resourcesExceeded',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
        self::assertStringContainsString('will have 10500 partitions', $e->getMessage());
        self::assertStringContainsString('Use coarser partitioning', $e->getMessage());
    }

    /**
     * @return Generator<string, array{PartitioningConfig, string}>
     */
    public static function provideTablePartitionLimitAdvice(): Generator
    {
        yield 'HOUR' => [
            new PartitioningConfig(new TimePartitioningConfig('HOUR', null, 'ts'), null, false),
            'Partition the table by DAY instead of HOUR.',
        ];
        yield 'DAY' => [
            new PartitioningConfig(new TimePartitioningConfig('DAY', null, 'ts'), null, false),
            'Partition the table by MONTH instead of DAY.',
        ];
        yield 'MONTH' => [
            new PartitioningConfig(new TimePartitioningConfig('MONTH', null, 'ts'), null, false),
            'Partition the table by YEAR instead of MONTH.',
        ];
        yield 'YEAR' => [
            new PartitioningConfig(new TimePartitioningConfig('YEAR', null, 'ts'), null, false),
            'Use a different partition column.',
        ];
        yield 'integer range' => [
            new PartitioningConfig(null, new RangePartitioningConfig('bucket', '0', '1000000', '10'), false),
            'Use a range interval larger than 10.',
        ];
    }

    #[DataProvider('provideTablePartitionLimitAdvice')]
    public function testCreateTablePartitionLimitExceptionNamesNextGranularity(
        PartitioningConfig $partitioning,
        string $expectedAdvice,
    ): void {
        $column = PartitionRangeColumn::fromDestination(
            $partitioning,
            new BigqueryTableDefinition('out.c-main', 'dest', false, new ColumnCollection([
                new BigqueryColumn('ts', new Bigquery(Bigquery::TYPE_TIMESTAMP)),
                new BigqueryColumn('bucket', new Bigquery(Bigquery::TYPE_INT64)),
            ]), []),
        );
        self::assertInstanceOf(PartitionRangeColumn::class, $column);

        $e = BigqueryException::createTablePartitionLimitException(10001, $column);

        self::assertSame(
            'The load holds 10001 partitions, BigQuery allows 10000 partitions in one table. ' . $expectedAdvice,
            $e->getMessage(),
        );
    }

    public function testCovertExceptionResourcesExceeded(): void
    {
        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                    'errors' => [
                        [
                            'message' => self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE,
                            'domain' => 'global',
                            'reason' => 'resourcesExceeded',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryResourcesExceededException::class, $e);
        // the user-error mapping in the storage driver hangs on this parent
        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        // the human sentence is extracted from the JSON body, the payload itself is not pasted in
        self::assertSame(self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE . ' ' . self::ADVICE, $e->getMessage());
        self::assertStringNotContainsString('{"error"', $e->getMessage());
        self::assertInstanceOf(BadRequestException::class, $e->getPrevious());
    }

    public function testCovertExceptionResourcesExceededOnlyByReason(): void
    {
        // a non-shuffle variant still classifies via reason, and the real cause stays visible
        $memoryMessage = 'Resources exceeded during query execution: The query could not be executed in the '
            . 'allotted memory. Peak usage: 136% of limit. Top memory consumer(s): ORDER BY operations: 100%';

        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => $memoryMessage,
                    'errors' => [
                        [
                            'message' => $memoryMessage,
                            'domain' => 'global',
                            'reason' => 'resourcesExceeded',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryResourcesExceededException::class, $e);
        self::assertSame($memoryMessage . ' ' . self::ADVICE, $e->getMessage());
        self::assertStringNotContainsString('{"error"', $e->getMessage());
    }

    public function testCovertExceptionResourcesExceededNonJsonMessageIsKeptIntact(): void
    {
        $e = BigqueryException::covertException(
            new BadRequestException('resourcesExceeded: plain text, not JSON', 400),
        );

        self::assertInstanceOf(BigqueryResourcesExceededException::class, $e);
        self::assertSame('resourcesExceeded: plain text, not JSON ' . self::ADVICE, $e->getMessage());
    }

    public function testCovertExceptionResourcesExceededPrefixAloneIsNotMatched(): void
    {
        // no shuffle tail and no resourcesExceeded reason -> stays a generic error
        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => 'Resources exceeded during query execution.',
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryException::class, $e);
        self::assertNotInstanceOf(BigqueryInputDataException::class, $e);
    }

    public function testCovertExceptionOtherServiceExceptionIsNotUserError(): void
    {
        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => 'Syntax error: Unexpected end of statement at [1:10]',
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryException::class, $e);
        self::assertNotInstanceOf(BigqueryInputDataException::class, $e);
    }

    public function testCovertExceptionMissingTimestampColumnIsUserError(): void
    {
        $message = 'Column _timestamp is not present in table in_c_main.my_table at [1:14962]';

        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => $message,
                    'errors' => [
                        [
                            'message' => $message,
                            'domain' => 'global',
                            'reason' => 'invalidQuery',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        self::assertSame($message, $e->getMessage());
        self::assertStringNotContainsString('{"error"', $e->getMessage());
        self::assertInstanceOf(BadRequestException::class, $e->getPrevious());
    }

    public function testCovertExceptionMissingTimestampColumnNonJsonMessageIsKeptIntact(): void
    {
        $e = BigqueryException::covertException(
            new BadRequestException('Column `_timestamp` is not present in table in_c_main.my_table', 400),
        );

        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        self::assertSame('Column `_timestamp` is not present in table in_c_main.my_table', $e->getMessage());
    }

    public function testCovertExceptionMissingDataColumnIsNotUserError(): void
    {
        $message = 'Column price is not present in table in_c_main.my_table at [1:14962]';

        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => $message,
                    'errors' => [
                        [
                            'message' => $message,
                            'domain' => 'global',
                            'reason' => 'invalidQuery',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryException::class, $e);
        self::assertNotInstanceOf(BigqueryInputDataException::class, $e);
    }

    public function testCovertExceptionTooManyPartitionsIsUserError(): void
    {
        // the exact body from DMD-2744
        $e = BigqueryException::covertException(new BadRequestException(
            (string) json_encode([
                'error' => [
                    'code' => 400,
                    'message' => self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE,
                    'errors' => [
                        [
                            'message' => self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE,
                            'domain' => 'global',
                            'reason' => 'invalidQuery',
                            'location' => 'q',
                            'locationType' => 'parameter',
                        ],
                    ],
                    'status' => 'INVALID_ARGUMENT',
                ],
            ]),
            400,
        ));

        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        self::assertNotInstanceOf(BigqueryResourcesExceededException::class, $e);
        self::assertSame(
            self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE . ' ' . self::TOO_MANY_PARTITIONS_ADVICE,
            $e->getMessage(),
        );
        self::assertStringNotContainsString('{"error"', $e->getMessage());
        self::assertInstanceOf(BadRequestException::class, $e->getPrevious());
    }

    public function testCovertExceptionTooManyPartitionsNonJsonMessageIsKeptIntact(): void
    {
        $e = BigqueryException::covertException(
            new BadRequestException(self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE, 400),
        );

        self::assertInstanceOf(BigqueryInputDataException::class, $e);
        self::assertSame(
            self::TOO_MANY_PARTITIONS_BIGQUERY_MESSAGE . ' ' . self::TOO_MANY_PARTITIONS_ADVICE,
            $e->getMessage(),
        );
    }
}
