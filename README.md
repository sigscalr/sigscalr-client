## Setup

To Run Ingestion of records on Hyperion:
```bash
$ go run main.go ingest -b 1000 -i ind -n 10_000 -u http://localhost:8081/elastic -p 2 -d 3600
```

To Query Records From Hyperion:
```bash
$ go run main.go query -u http://localhost:8081/elastic
```

To Run Ingest Followed By Query on Hyperion:
```bash
$ go run main.go ingestThenQuery -b 10000 -i ind -n 1_000_000 -u http://localhost:8081/elastic
```

To Compare Result's between Hyperion and ElasticSearch
```bash
$ go run main.go ingestThenQuery -b 10000 -i ind -n 1_000_000 -e http://localhost:9200 -u http://localhost:8081/elastic
```


## Flags
```
Flags:
  -b, --b int          Batch size (default 1000)
  -d, --duration int   Time Duration is seconds for event range (default 3600)
  -e, --esUrl string   ES URL
  -f, --file string    file prefix to read the events from to ingest engine (default "ab")
  -u, --hUrl string    Hyperion URL
  -h, --help           help for ingest
  -i, --ind string     index suffix (default "default")
  -n, --n int          Total number of events (default 1000000)
  -p, --process int    Number of parallel process to ingest data from different files. (default 1)
```