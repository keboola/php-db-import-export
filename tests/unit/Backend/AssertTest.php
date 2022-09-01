<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use PHPUnit\Framework\TestCase;

class AssertTest extends TestCase
{
    public function testAssertSameColumns(): void
    {
        $this->expectNotToPerformAssertions();
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];

        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidCountExtraSource(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            SnowflakeColumn::createGenericColumn('test2'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Tables don\'t have same number of columns.');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidCountExtraDestination(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            SnowflakeColumn::createGenericColumn('test2'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Tables don\'t have same number of columns.');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidColumnName(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1x'),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns name mismatch. "test1x"->"test1"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidType(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIMESTAMP_NTZ,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            'Source destination columns mismatch. "test2 TIME (3)"->"test2 TIMESTAMP_NTZ (3)"'
        );
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertSameColumnsInvalidLength(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '3',
                    ]
                )
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '4',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME (3)"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }


    public function testAssertSameColumnsInvalidLength2(): void
    {
        $sourceCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(Snowflake::TYPE_TIME)
            ),
        ];
        $destCols = [
            SnowflakeColumn::createGenericColumn('test'),
            SnowflakeColumn::createGenericColumn('test1'),
            new SnowflakeColumn(
                'test2',
                new Snowflake(
                    Snowflake::TYPE_TIME,
                    [
                        'length' => '4',
                    ]
                )
            ),
        ];

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Source destination columns mismatch. "test2 TIME"->"test2 TIME (4)"');
        Assert::assertSameColumns(
            new ColumnCollection($sourceCols),
            new ColumnCollection($destCols)
        );
    }

    public function testAssertColumnsOnTableDefinitionPass(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertColumnsOnTableDefinition(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['name', 'id'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionNoColumnsFail(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('No columns found in CSV file.');
        Assert::assertColumnsOnTableDefinition(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return [];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionNoColumnsFailCaseSensitive(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: iD');
        Assert::assertColumnsOnTableDefinition(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['iD','NAME'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionNoColumnsNotMatch(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: unexpected');
        Assert::assertColumnsOnTableDefinition(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['name', 'id', 'unexpected'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionCaseInsensitivePass(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertColumnsOnTableDefinitionCaseInsensitive(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['name', 'id'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionCaseInsensitiveNoColumnsFail(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('No columns found in CSV file.');
        Assert::assertColumnsOnTableDefinitionCaseInsensitive(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return [];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionCaseInsensitiveNoColumnsFailCaseSensitive(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertColumnsOnTableDefinitionCaseInsensitive(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['iD','NAME'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }

    public function testAssertColumnsOnTableDefinitionCaseInsensitiveNoColumnsNotMatch(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: UNEXPECTED');
        Assert::assertColumnsOnTableDefinitionCaseInsensitive(
            new class implements SourceInterface {
                /** @return string[] */
                public function getColumnsNames(): array
                {
                    return ['name', 'id', 'unexpected'];
                }
                /** @return string[]|null */
                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new SnowflakeTableDefinition(
                '',
                '',
                true,
                new ColumnCollection([
                    SnowflakeColumn::createGenericColumn('id'),
                    SnowflakeColumn::createGenericColumn('name'),
                ]),
                []
            )
        );
    }
}
