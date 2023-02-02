package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"verifier/pkg/utils"

	"github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
)

type IngestType int

// Node Types
const (
	_ IngestType = iota
	ESBulk
	OpenTSDB
)

func (q IngestType) String() string {
	switch q {
	case ESBulk:
		return "ES Bulk"
	case OpenTSDB:
		return "OTSDB"
	default:
		return "UNKNOWN"
	}
}

const PRINT_FREQ = 100_000

var actionLines []string = []string{}

func sendRequest(iType IngestType, client *http.Client, lines []byte, url string) {

	buf := bytes.NewBuffer(lines)

	var requestStr string
	switch iType {
	case ESBulk:
		requestStr = url + "/_bulk"
	case OpenTSDB:
		requestStr = url + "/api/put"
	default:
		log.Fatalf("unknown ingest type %+v", iType)
	}

	req, err := http.NewRequest("POST", requestStr, buf)

	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
}

func generateBody(iType IngestType, recs int, i int, rdr utils.Generator) ([]byte, error) {
	switch iType {
	case ESBulk:
		actionLine := getActionLine(i)
		return generateESBody(recs, actionLine, rdr)
	case OpenTSDB:
		return generateOpenTSDBBody(recs, rdr)
	default:
		log.Fatalf("Unsupported ingest type %s", iType.String())
	}
	return nil, fmt.Errorf("unsupported ingest type %s", iType.String())
}

func generateESBody(recs int, actionLine string, rdr utils.Generator) ([]byte, error) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	for i := 0; i < recs; i++ {
		_, _ = bb.WriteString(actionLine)
		logline, err := rdr.GetLogLine()
		if err != nil {
			return nil, err
		}
		_, _ = bb.Write(logline)
		_, _ = bb.WriteString("\n")
	}
	payLoad := bb.Bytes()
	return payLoad, nil
}

func generateOpenTSDBBody(recs int, rdr utils.Generator) ([]byte, error) {
	finalPayLoad := make([]interface{}, recs)
	for i := 0; i < recs; i++ {
		currPayload, err := rdr.GetRawLog()
		if err != nil {
			return nil, err
		}
		finalPayLoad[i] = currPayload
	}
	retVal, err := json.Marshal(finalPayLoad)
	if err != nil {
		return nil, err
	}
	return retVal, nil
}

func getActionLine(i int) string {
	return actionLines[i%len(actionLines)]
}

func runIngestion(iType IngestType, rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents int,
	continous bool, batchSize, processNo int, indexSuffix string, ctr *uint64) {
	defer wg.Done()
	eventCounter := 0
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 500
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100
	client := &http.Client{
		Timeout:   100 * time.Second,
		Transport: t,
	}

	i := 0
	for continous || eventCounter < totalEvents {

		recsInBatch := batchSize
		if !continous && eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		i++
		payload, err := generateBody(iType, recsInBatch, i, rdr)
		if err != nil {
			log.Errorf("Error generating bulk body!: %v", err)
			return
		}
		sendRequest(iType, client, payload, url)
		eventCounter += recsInBatch
		atomic.AddUint64(ctr, uint64(recsInBatch))
	}
}

func populateActionLines(idxPrefix string, indexName string, numIndices int) {
	if numIndices == 0 {
		log.Fatalf("number of indices cannot be zero!")
	}
	actionLines = make([]string, numIndices)
	for i := 0; i < numIndices; i++ {
		var idx string
		if indexName != "" {
			idx = indexName
		} else {
			idx = fmt.Sprintf("%s-%d", idxPrefix, i)
		}
		actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
		actionLines[i] = actionLine
	}
}

func getReaderFromArgs(iType IngestType, nummetrics int, gentype, str string, ts bool) (utils.Generator, error) {

	if iType == OpenTSDB {
		rdr := utils.InitMetricsGenerator(nummetrics)
		err := rdr.Init(str)
		return rdr, err
	}

	var rdr utils.Generator
	switch gentype {
	case "", "static":
		log.Infof("Initializing static reader")
		rdr = utils.InitStaticGenerator(ts)
	case "dynamic-user":
		rdr = utils.InitDynamicUserGenerator(ts)
	case "file":
		log.Infof("Initializing file reader from %s", str)
		rdr = utils.InitFileReader()
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,dynamic,file]", gentype)
	}
	err := rdr.Init(str)
	return rdr, err
}

func StartIngestion(iType IngestType, generatorType, dataFile string, totalEvents int, continuous bool,
	batchSize int, url string, indexPrefix string, indexName string, numIndices, processCount int, addTs bool, nMetrics int) {
	log.Printf("Starting ingestion at %+v for %+v", url, iType.String())
	var wg sync.WaitGroup
	totalEventsPerProcess := totalEvents / processCount

	ticker := time.NewTicker(time.Minute)
	done := make(chan bool)
	totalSent := uint64(0)

	if iType == ESBulk {
		populateActionLines(indexPrefix, indexName, numIndices)
	}

	for i := 0; i < processCount; i++ {
		wg.Add(1)
		reader, err := getReaderFromArgs(iType, nMetrics, generatorType, dataFile, addTs)
		if err != nil {
			log.Fatalf("StartIngestion: failed to initalize reader! %+v", err)
		}
		go runIngestion(iType, reader, &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix, &totalSent)
	}

	go func() {
		wg.Wait()
		done <- true
	}()
	startTime := time.Now()

	lastPrintedCount := uint64(0)
readChannel:
	for {
		select {
		case <-done:
			break readChannel
		case <-ticker.C:
			totalTimeTaken := time.Since(startTime)
			eventsPerSec := int64((totalSent - lastPrintedCount) / 60)
			log.Infof("Total elapsed time:%s. Total sent events %+v. Events per second:%+v", totalTimeTaken, humanize.Comma(int64(totalSent)), humanize.Comma(eventsPerSec))
			if iType == OpenTSDB {
				log.Infof("Approximation of sent number of unique timeseries:%+v", utils.GetMetricsHLL())
			}
			lastPrintedCount = totalSent
		}
	}
	log.Printf("Total events ingested:%+d. Event type: %s", totalEvents, iType.String())
	totalTimeTaken := time.Since(startTime)

	numSeconds := int(totalTimeTaken.Seconds())
	if numSeconds == 0 {
		log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
	} else {
		eventsPerSecond := int64(totalEvents / numSeconds)
		log.Printf("Total Time Taken for ingestion %s. Average events per second=%+v", totalTimeTaken, humanize.Comma(eventsPerSecond))
	}
}
