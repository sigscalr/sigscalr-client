## Setup

To Run Ingestion of records on Hyperion:
```bash
$ go run main.go ingest -n 10_000 -d http://localhost:8081/elastic -p 2
```

## Flags
```
Flags:
  -b, --b int          Batch size (default 100)
  -f, --file string    file prefix to read the events from to ingest engine (default "ab")
  -d, --d string       Destination URL. The client will add `/_bulk`
  -h, --help           help for ingest
  -i, --ind string     index name (default "index")
  -n, --n int          Total number of events (default 1000000)
  -p, --process int    Number of parallel process to ingest data from different files. (default 1)
```