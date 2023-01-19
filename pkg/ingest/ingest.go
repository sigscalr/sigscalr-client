package ingest

import (
	"bytes"
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

const PRINT_FREQ = 100_000

var actionLines []string = []string{}

func sendRequest(client *http.Client, lines string, url string) {

	buf := bytes.NewBuffer([]byte(lines))

	requestStr := url + "/_bulk"

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

// var frand fastrand.RNG

func generateBulkBody(recs int, actionLine string, rdr utils.Generator) (string, error) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	for i := 0; i < recs; i++ {
		_, _ = bb.WriteString(actionLine)
		logline, err := rdr.GetLogLine()
		if err != nil {
			return "", err
		}
		_, _ = bb.Write(logline)
		_, _ = bb.WriteString("\n")
	}
	payLoad := bb.String()
	return payLoad, nil
}

func getActionLine(i int) string {
	return actionLines[i%len(actionLines)]
}

func runIngestion(rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents, batchSize, processNo int, indexSuffix string, ctr *uint64) {
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
	for eventCounter < totalEvents {

		recsInBatch := batchSize
		if eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		actionLine := getActionLine(i)
		i++
		payload, err := generateBulkBody(recsInBatch, actionLine, rdr)
		if err != nil {
			log.Errorf("Error generating bulk body!: %v", err)
			return
		}
		sendRequest(client, payload, url)
		eventCounter += recsInBatch
		atomic.AddUint64(ctr, uint64(recsInBatch))
	}
}

func populateActionLines(idxPrefix string, numIndices int) {
	if numIndices == 0 {
		log.Fatalf("number of indices cannot be zero!")
	}
	actionLines = make([]string, numIndices)
	for i := 0; i < numIndices; i++ {
		idx := fmt.Sprintf("%s-%d", idxPrefix, i)
		actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
		actionLines[i] = actionLine
	}
}

func getReaderFromArgs(gentype, str string, ts bool) (utils.Generator, error) {
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

func StartIngestion(generatorType, dataFile string, totalEvents int, batchSize int, url string,
	indexPrefix string, numIndices, processCount int, addTs bool) {
	log.Println("Starting ingestion at ", url, "...")
	var wg sync.WaitGroup
	totalEventsPerProcess := totalEvents / processCount

	ticker := time.NewTicker(time.Minute)
	done := make(chan bool)
	totalSent := uint64(0)
	populateActionLines(indexPrefix, numIndices)

	for i := 0; i < processCount; i++ {
		wg.Add(1)
		reader, err := getReaderFromArgs(generatorType, dataFile, addTs)
		if err != nil {
			log.Fatalf("StartIngestion: failed to initalize reader! %+v", err)
		}
		go runIngestion(reader, &wg, url, totalEventsPerProcess, batchSize, i+1, indexPrefix, &totalSent)
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
			lastPrintedCount = totalSent
		}
	}
	log.Println("Total logs ingested: ", totalEvents)
	totalTimeTaken := time.Since(startTime)

	numSeconds := int(totalTimeTaken.Seconds())
	if numSeconds == 0 {
		log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
	} else {
		eventsPerSecond := int64(totalEvents / numSeconds)
		log.Printf("Total Time Taken for ingestion %s. Average events per second=%+v", totalTimeTaken, humanize.Comma(eventsPerSecond))
	}
}
