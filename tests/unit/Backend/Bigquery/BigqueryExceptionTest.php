<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery;

use Generator;
use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryResourcesExceededException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

class BigqueryExceptionTest extends TestCase
{
    // phpcs:ignore Generic.Files.LineLength
    private const RESOURCES_EXCEEDED_BIGQUERY_MESSAGE = 'Resources exceeded during query execution: Your project or organization exceeded the maximum disk and memory limit available for shuffle operations. Consider provisioning more slots, reducing query concurrency, or using more efficient logic in this job.';

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
                self::assertStringContainsString('shuffle operations', $e->getMessage());
                self::assertStringContainsString('delete_where', $e->getMessage());
                // the original BigQuery text and the job id must survive for support
                self::assertStringContainsString(self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE, $e->getMessage());
                self::assertStringContainsString('eb64d133-213d-4e99-9cc0-4e37d0c18de9', $e->getMessage());
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
        self::assertStringContainsString('shuffle operations', $e->getMessage());
        self::assertStringContainsString('retention', $e->getMessage());
        self::assertStringContainsString('delete_where', $e->getMessage());
        // the raw BigQuery payload and the original exception must survive for support
        self::assertStringContainsString(self::RESOURCES_EXCEEDED_BIGQUERY_MESSAGE, $e->getMessage());
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
        self::assertStringContainsString('ORDER BY operations', $e->getMessage());
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
}
