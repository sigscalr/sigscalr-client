## Setup

To send ingestion traffic to a server using ES Bulk API :
```bash
$ go run main.go ingest -n 10_000 -d http://localhost:8081/elastic -p 2
```

## Flags
```
Flags:
  -b, --batchSize int        Batch size (default 100)
  -d, --dest string          Destination URL. Client will append /_bulk
  -x, --filePath string      path to json file containing loglines to send to server
  -h, --help                 help for ingest
  -i, --indexPrefix string   ES index prefix (default "ind")
  -n, --numIndices int       number of indices to ingest to (default 1)
  -p, --processCount int     Number of parallel process to ingest data from. (default 1)
  -t, --totalEvents int      Total number of events to send (default 1000000)
```
