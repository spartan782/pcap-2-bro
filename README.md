# pcap-2-bro
Replay PCAP through a rock sensor

### Command Line Options
* -w --workers `NUMBER` (number of threads to allocate to pool)
* -d --directory `/path/to/dir` (top directory if using the -r option or directory that contains Pcap files)
* -r --recursive (if you have multiple pcaps in layered directories)
* -f --file `/path/to/pcap/file` (single pcap ingest)
* -v --verbose (use if you want status updates every 10 seconds on screen)
* -s --source `UUID` or `Source Name`. This replaces the ROCK::sensor_id variable so that you can search for the data easily inside of ROCK and identify the source it came from.
