# SigScalr Client

## Ingest

To send ingestion traffic to a server using ES Bulk API:
```bash
$ ./load-test ingest -n 10_000 -d http://localhost:8081/elastic -p 2
```
Options:
```
  -b, --batchSize int        Batch size (default 100)
  -d, --dest string          Destination URL. Client will append /_bulk
  -g, --generator string     type of generator to use. Options=[static,dynamic-user,file]. If file is selected, -x/--filePath must be specified (default "static")
  
  -x, --filePath string      path to json file containing loglines to send to server
  -h, --help                 help for ingest
  -i, --indexPrefix string   Index prefix to ingest (default "ind")
  -n, --numIndices int       number of indices to ingest to (default 1)
  -p, --processCount int     Number of parallel process to ingest data from. (default 1)
  -t, --totalEvents int      Total number of events to send (default 1000000)
  -s, --timestamp            If set, adds "timestamp" to the static/dynamic generators

  -c  continuous             If true, ignores -t and will continuously send docs to the destination
```

Different Types of Readers:

1. Static: Sends the same payload over and over
2. Dynamic User: Randomly Generates user events. These random events are generated using [gofakeit](github.com/brianvoe/gofakeit/v6).
3. File: Reads a file line by line. Expects each line is a new json. Will loop over file if necessary

## Query

To send queries and measure responses to a server:
```bash
$ ./load-test query -d http://localhost:8081/elastic -v
```


Options:
```
-d, --dest string          Destination URL. Client will append /{indexPrefix}*/_search
-i, --indexPrefix string   Index prefix to search (default "ind")
-n, --numIterations int    Number of iterations to send query suite (default 10)

-v  verbose                Output hits and elapsed time for each query
-c  continuous             If true, ignores -n and -v and will continuously send queries to the destination and will log results
```

## Generating traces
To generate synthetic traces: 
```bash
$ ./load-test traces -f test.json -t 1000 -s 10
```

Options:
```
-f, --filePath string          path to json file to output traces to 
-t, --totalEvents int          number of total traces to generate
-s, --maxSpans int             number of max spans for each trace
```

## Utils

To convert a TSV to a JSON file that can be ingested via `-g dynamic -f file`:
```bash
$ go run cmd/utils/converter.go --input {input file name} --output {output file name}
```
