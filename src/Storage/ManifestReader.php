<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Generator;
use pcrov\JsonReader\JsonReader;

class ManifestReader
{
    /**
     * @param resource $stream
     */
    public static function readEntries(
        $stream
    ): Generator {
        $reader = new JsonReader();
        $reader->stream($stream);

        $reader->read('entries');
        $depth = $reader->depth(); // Check in a moment to break when the array is done.

        $reader->read(); // Step to the first element.
        do {
            if ($reader->value() === null) {
                // on empty entries lib will return null item
                continue;
            }
            yield $reader->value();
        } while ($reader->next() && $reader->depth() > $depth); // Read each sibling.

        $reader->close();
    }
}
