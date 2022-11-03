package verifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fastrand"
)

const INDEX = "verifier"

const PRINT_FREQ = 100_000

func sendRequest(lines string, url string) {

	buf := bytes.NewBuffer([]byte(lines))

	requestStr := url + "/_bulk"

	req, err := http.NewRequest("POST", requestStr, buf)

	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}

	client := &http.Client{}

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

var numCols = 4
var frand fastrand.RNG

func updateMockBody(m map[string]interface{}) {
	m["col1"] = fmt.Sprintf("batch-%d", frand.Uint32n(1001))
	m["col2"] = frand.Uint32n(200)
	m["col3"] = "abc"
	m["col4"] = "abc def"
}

func generateBulkBody(recs int, idx string) string {
	actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	m := make(map[string]interface{}, numCols)
	for i := 0; i < recs; i++ {
		bb.WriteString(actionLine)
		updateMockBody(m)
		retVal, err := json.Marshal(&m)
		if err != nil {
			log.Fatalf("Error marshalling mock body %+v", err)
		}
		bb.Write(retVal)
	}
	retVal := bb.String()
	return retVal
}

func runIngestion(wg *sync.WaitGroup, url string, totalEvents, batchSize, processNo int, indexSuffix string) {
	defer wg.Done()
	eventCounter := 0
	indexName := fmt.Sprintf("%v-%v", indexSuffix, processNo)
	for eventCounter < totalEvents {

		recsInBatch := batchSize
		if eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		payload := generateBulkBody(recsInBatch, indexName)
		sendRequest(payload, url)
		eventCounter += recsInBatch
	}

	log.Infof("Process=%d Finished ingestion of Total Records %d", processNo, totalEvents)
}

func StartIngestion(totalEvents int, batchSize int, url string, indexSuffix string, processCount int) {
	log.Println("Starting ingestion at ", url, "...")
	var wg sync.WaitGroup
	startTime := time.Now()
	totalEventsPerProcess := totalEvents / processCount
	for i := 0; i < processCount; i++ {
		wg.Add(1)
		go runIngestion(&wg, url, totalEventsPerProcess, batchSize, i+1, indexSuffix)
	}
	wg.Wait()
	log.Println("Total logs ingested: ", totalEvents)
	totalTimeTaken := time.Since(startTime)
	log.Printf("Total Time Taken for ingestion %+v seconds", totalTimeTaken)
}
