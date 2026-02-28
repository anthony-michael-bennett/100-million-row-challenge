<?php
#ini_set('memory_limit', '1024M'); // or '1G'
namespace App;

use Exception;

final class Parser
{

    # Get the start and end indexes. Look for newlines to avoid splitting lines.
    public function get_chonks($input, $num_chonks, $read_file=true) {
        if ($read_file) {
            $size = filesize($input);
            $handle = fopen($input, "r");
        } else {
            $size = strlen($input);
        }
        
        $slice = floor($size / $num_chonks);
        $pos = 0;

        $chonks = [];
        $last_pos = 0;

        for ($i = 0; $i < $num_chonks; $i++) {

            $chunk_end = min($size, $pos + $slice);

            if ($read_file) {

                $pos = $chunk_end;

                if (fseek($handle, $chunk_end) !== -1) {
                
                    while(!feof($handle)) {
                        $buffer = fread($handle, 1024);
                        $tokenpos = strpos($buffer, "\n");
                        if ($tokenpos === false) {
                            $pos += 1024;
                        } else {
                            $pos += $tokenpos;// + 1
                            break;
                        }
                    }
                    if ($pos > $size) {
                        $pos = $size;
                    }
                }
            } else {
                $pos = strpos($input, "\n", $chunk_end);
                if ($pos === false) $pos = $size;
            }

            $chonks[] = [$last_pos, $pos];
            $last_pos = $pos+1;
        }
        if ($read_file) {
            fclose($handle);
        }

        return $chonks;
    }

    public function create_time_buckets($start_year, $end_year) {

        $years = $end_year - $start_year + 1;
        $size = ($years * 12 * 31);
        $arr = new \SplFixedArray($size);
        $index_to_date = clone $arr;
        $date_to_index = [];

        $index = 0;
        for ($i = 0; $i < 6; $i++) {
            for ($j = 0; $j < 12; $j++) {
                for ($k = 0; $k < 31; $k++) {
                    $arr[$index] = 0;
                    $date_value = sprintf('%d-%02d-%02d', $i+$start_year, $j+1, $k+1);
                    $index_to_date[$index] = $date_value;
                    $date_to_index[$date_value] = $index;
                    $index++;
                }
            }
        }

        return [
            "buckets" => $arr,
            "index_to_date" => $index_to_date,
            "date_to_index" => $date_to_index,
        ];
    }

    public function create_workers($inputPath, $num_workers, $time_buckets) {

        // PHP has copy-on-write. No extra memory for read then fork.
        // This isn't more efficient for me. It might be in some situations.

        $read_file_in_parent = false;

        if ($read_file_in_parent) {
            $file_contents = file_get_contents($inputPath);
            $chonks = $this->get_chonks($file_contents, $num_workers, false);
        } else {    
            $file_contents = null;
            $chonks = $this->get_chonks($inputPath, $num_workers);
        }

        $workers = array_fill(0, $num_workers, null);

        for ($i = 0; $i < $num_workers; $i++) {
            # create a place to save/get the playload
            $shm_key = $this->gen_shm_key($i);
            $this->write_shared_key($shm_key, "running", "c");
            $pid = pcntl_fork();

            if ($pid == -1) {
                die('could not fork');
            } else if ($pid == 0) {

                // child

                [$pos, $endpos] = $chonks[$i];

                if ($file_contents === null) {
                    $handle = fopen($inputPath, "r");
                    fseek($handle, $pos, SEEK_SET);
                    $file_contents = fread($handle, $endpos - $pos);
                    $endpos = strlen($file_contents);
                    $pos = 0;
                    fclose($handle);
                }

                $this->worker($pos, $endpos, $shm_key, $time_buckets, $file_contents, $inputPath);
                exit(0);            

            } else {

                // parent

                $workers[$i] =  [
                    "pid" => $pid,
                    "shm_key" => $shm_key,
                    "running" => true
                ];
            }
        }
        return $workers;
    }

    public function worker($pos, $endpos, $shm_key, $time_buckets, $file_contents) {

        // process
        while ($pos < $endpos) {
            $next_newline = strpos($file_contents, "\n", $pos) ?: $endpos;
            $line = substr($file_contents, $pos, $next_newline-$pos);
            $pos = $next_newline+1;
            // The date format is same length so we can use that instead of looking for comma.
            // Using negatives we can substr from relation to end of string.
            # 2024-05-26T19:20:37+00:00
            $url = substr($line, 19, -26);
            $date = substr($line, -25, 10);

            # we need to check if the url has buckets and if not set to clone
            $data[$url] ??= clone $time_buckets["buckets"];

            # Fixed arrays are faster so we used a fixed number of slots for
            # counts based on last five or six years.
            $date_index = $time_buckets["date_to_index"][$date];
            $data[$url][$date_index] = $data[$url][$date_index] + 1;
        }

        $this->write_shared_key($shm_key, $data);
    }

    public function gen_shm_key($num) {
        return ftok(__FILE__, chr(65 + $num));
    }

    // use mode c for create
    public function write_shared_key($shm_key, $data, $mode = "w") {
        $serializedData = json_encode($data);
        $size = strlen($serializedData);

        // 1. Open/Create and write (Resize by recreating if needed)
        if ($mode === "w") {
            $shm_id = shmop_open($shm_key, $mode, 0, 0); // Try to open existing
            if ($shm_id) {
                shmop_delete($shm_id); // Delete existing
            }
        }
        
        $shm_id = shmop_open($shm_key, "c", 0644, $size); // Create new with correct size

        // 2. Write data
        shmop_write($shm_id, $serializedData, 0);
    }

    public function read_shared_key($shm_key) {
        $shm_id = shmop_open($shm_key, "a", 0, 0);
        if ($shm_id === false) {
            die('Failed to open shared memory segment');
        }
        $data = shmop_read($shm_id, 0, shmop_size($shm_id));
        shmop_delete($shm_id);
        return json_decode($data, true);
    }

    public function worker_status($worker) {
        $pid = $worker["pid"];
        $status = null;
        $result = pcntl_waitpid($pid, $status, WNOHANG);
        if ($result == 0) {
            return "running";
        } else if ($result == -1) {
            return "exited";
        } else {
            return "exited";
        }
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $data = [];
        $num_workers = 7;
        $end_year = date("Y", time());
        $start_year = $end_year - 5;
        $time_buckets = $this->create_time_buckets($start_year, $end_year);
        $workers = $this->create_workers($inputPath, $num_workers, $time_buckets);

         // wait for workers
         // - process worker payload as soon as any worker has finished
         // - read data from shared memory only when worker is done to avoid locking and waiting

        while(count($workers) > 0) {
            $status = null;
            $check_pid = pcntl_wait($status, WUNTRACED); // blocks until a worker changes state (exits, is signaled, or is stopped)

            // There was a change of state to a child pid. Was it a worker?
            $worker_key = array_find_key($workers, function($w) use ($check_pid) {
                return $w["pid"] == $check_pid;
            });

            if ($worker_key === null) {
                continue; // Not a worker, ignore.
            }

            $worker = $workers[$worker_key];
            
            if (pcntl_wifexited($status)) {                
                // worker is done. Remove from workers and read data.
                unset($workers[$worker_key]);
                $process_data = $this->read_shared_key($worker["shm_key"]);

                foreach ($process_data as $key=>$value) {
                    // use worker data as counts when url isn't set
                    if (!isset($data[$key])) {  
                        $data[$key] = $value;
                        continue;
                    }

                    // add counts to existing data
                    foreach($value as $i=>$v) {
                        $data[$key][$i] += $v;
                    }
                }

            } elseif (pcntl_wifsignaled($status)) {
                // killed by signal
                // attempt to kill process if still running and log error
                unset($workers[$worker_key]);
            } elseif (pcntl_wifstopped($status)) {
                // stopped by signal
                // attempt to kill process if still running and log error
                unset($workers[$worker_key]);
            }
        }
        
        $index_to_date = $time_buckets["index_to_date"];

        // convert from the fast indexed fixed arrays to date keys, remove zeros counts.
        foreach ($data as $key=>$value) {
            $new_array = [];
            foreach($value as $i=>$v) {
                if ($v !== 0 && $v !== null) {
                    $new_array[$index_to_date[$i]] = $v;
                }
            }
            $data[$key] = $new_array;
        }

        $jsonHandle = fopen($outputPath, 'w');
        fwrite($jsonHandle, json_encode($data, JSON_PRETTY_PRINT));
    }
}