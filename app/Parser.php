<?php
#ini_set('memory_limit', '1024M'); // or '1G'
namespace App;

use Exception;

final class Parser
{

    public function parse(string $inputPath, string $outputPath): void
    {
        # Using SplFileObject. It's supposed to be better for large files.
        # Not using the builtin CSV parser because it's too slow.
        $file = new \SplFileObject($inputPath, 'r');
        $data = [];

        $stime = microtime(true);
        $timetaken = 0;

        // six years, twelve months, 31 possible days
        $fixed_array_size = (6 * 12 * 31);
        $fixed_array = new \SplFixedArray($fixed_array_size);
        $fixed_array_index_map = clone $fixed_array;
        $fixed_array_index_map_reverse = [];

        array_fill(0, $fixed_array_size, 0);

        $fixed_array_index = 0;
        for ($i = 0; $i < 6; $i++) {
            for ($j = 0; $j < 12; $j++) {
                for ($k = 0; $k < 31; $k++) {
                    $fixed_array[$fixed_array_index] = 0;
                    $date_value = sprintf('%d-%02d-%02d', $i+2021, $j+1, $k+1);
                    $fixed_array_index_map[$fixed_array_index] = $date_value;
                    $fixed_array_index_map_reverse[$date_value] = $fixed_array_index;
                    $fixed_array_index++;
                }
            }
        }

        while (!$file->eof()) {
            $line = $file->fgets();
            if ($line === "") continue;
            // The date format is same length so we can use that instead of looking for comma.
            // Using negatives we can substr from relation to end of string.
            # 2024-05-26T19:20:37+00:00
            $url = substr($line, 19, -27);
            $date = substr($line, -26, 10);

            # We don't have prior knowledge of the url. We could look
            # into Commands and the generator but that's cheating I think.
            $data[$url] ??= clone $fixed_array;

            # Fixed arrays are faster so we used a fixed number of slots for
            # counts based on last five or six years.
            $date_index = $fixed_array_index_map_reverse[$date];
            $data[$url][$date_index] = $data[$url][$date_index] + 1;
        }

        $jsonHandle = fopen($outputPath, 'w');

        foreach ($data as $key=>$value) {
            $new_array = [];
            for ($i = 0; $i < $fixed_array_size; $i++) {
                $this_value = $value[$i];
                if ($this_value !== 0 && $this_value !== null) {
                    $new_array[$fixed_array_index_map[$i]] = $this_value;
                }
            }
            $data[$key] = $new_array;
        }

        fwrite($jsonHandle, json_encode($data, JSON_PRETTY_PRINT));
    }
}