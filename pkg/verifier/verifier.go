package verifier

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
)

const INDEX = "verifier"

const PRINT_FREQ = 100_000

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

var numCols = 105

// var frand fastrand.RNG

func getMockBody() map[string]interface{} {
	ev := make(map[string]interface{})
	ts := time.Now().Unix()
	ev["logset"] = "t1"
	ev["traffic_flags"] = 8193
	ev["high_res_timestamp"] = ts
	ev["parent_start_time"] = ts
	ev["inbound_if"] = "1103823372288"
	ev["pod_name"] = "tam-dp-77754f4"
	ev["dstloc"] = "west-coast"
	ev["natdport"] = 17856
	ev["time_generated"] = ts
	ev["vpadding"] = 0
	ev["sdwan_fec_data"] = 0
	ev["chunks_sent"] = 67
	ev["offloaded"] = 0
	ev["dst_model"] = "S9"
	ev["to"] = "ethernet4Zone-test4"
	ev["monitor_tag_imei"] = "US Social"
	ev["xff_ip"] = "00000000000000000000ffff02020202"
	ev["dstuser"] = "funccompanysaf3ti"
	ev["seqno"] = 6922966563614901991
	ev["tunneled-app"] = "gtpv1-c"
	return ev
}

func generateBulkBody(recs int, body []byte, idx string) string {
	actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	for i := 0; i < recs; i++ {
		bb.WriteString(actionLine)
		bb.Write(body)
	}
	payLoad := bb.String()
	return payLoad
}

func runIngestion(wg *sync.WaitGroup, url string, totalEvents, batchSize, processNo int, indexSuffix string, ctr *uint64) {
	defer wg.Done()
	eventCounter := 0
	indexName := fmt.Sprintf("%v", indexSuffix)
	m := getMockBody()
	body, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Error marshalling mock body %+v", err)
	}

	for eventCounter < totalEvents {

		recsInBatch := batchSize
		if eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		payload := generateBulkBody(recsInBatch, body, indexName)
		sendRequest(payload, url)
		eventCounter += recsInBatch
		atomic.AddUint64(ctr, uint64(recsInBatch))
	}

	log.Infof("Process=%d Finished ingestion of Total Records %d", processNo, totalEvents)
}

func StartIngestion(totalEvents int, batchSize int, url string, indexSuffix string, processCount int) {
	log.Println("Starting ingestion at ", url, "...")
	var wg sync.WaitGroup
	startTime := time.Now()
	totalEventsPerProcess := totalEvents / processCount

	ticker := time.NewTicker(time.Minute)
	done := make(chan bool)
	totalSent := uint64(0)
	for i := 0; i < processCount; i++ {
		wg.Add(1)
		go runIngestion(&wg, url, totalEventsPerProcess, batchSize, i+1, indexSuffix, &totalSent)
	}

	go func() {
		wg.Wait()
		done <- true
	}()

	lastPrintedCount := uint64(0)
readChannel:
	for {
		select {
		case <-done:
			break readChannel
		case <-ticker.C:
			totalTimeTaken := time.Since(startTime)
			eventsPerMin := totalSent - lastPrintedCount
			log.Infof("Total elapsed time: %+v. Total sent events %+v. Events per minute %+v", totalTimeTaken, totalSent, eventsPerMin)
			lastPrintedCount = totalSent
		}
	}
	log.Println("Total logs ingested: ", totalEvents)
	totalTimeTaken := time.Since(startTime)
	log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
}
