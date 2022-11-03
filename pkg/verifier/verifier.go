package verifier

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const INDEX = "verifier"

const PRINT_FREQ = 100_000

func sendRequest(lines string, url string) {

	buf := bytes.NewBuffer([]byte(lines))

	requestStr := url + "/_bulk?pretty=true"

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

func runIngestion(wg *sync.WaitGroup, fileName, url string, totalEvents, batchSize, processNo, timestamp, timeIncrement, eventIncrement int, indexSuffix string) {
	defer wg.Done()
	var event map[string]interface{}

	eventCounter := 0
	var lines string

	fr, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("Process=%d Failed to open name=%v, err:%s", processNo, fileName, err)
		return
	}
	defer fr.Close()

	indexName := fmt.Sprintf("%v-v%v", indexSuffix, processNo)
	for eventCounter < totalEvents {
		reader := bufio.NewScanner(fr)
		for reader.Scan() {
			lines += "{\"index\": {\"_index\": \"" + indexName + "\", \"_type\": \"_doc\"}}\n"
			err = json.Unmarshal(reader.Bytes(), &event)
			if err != nil {
				log.Errorf("Process= %d failed to unmarshal record err: %s", processNo, err.Error())
			}
			event["timestamp"] = timestamp
			data, err := json.Marshal(event)
			if err != nil {
				log.Errorf("Process= %d failed to matshal record err: %s", processNo, err.Error())
			}
			lines += string(data) + "\n"

			if eventCounter%batchSize == 0 {
				sendRequest(lines, url)
				if eventCounter%PRINT_FREQ == 0 {
					log.Infof("Process=%d Total logs ingested so far %d", processNo, eventCounter)
				}
				lines = ""
			}
			eventCounter++
			if eventCounter >= totalEvents {
				break
			}
			if eventCounter%eventIncrement == 0 {
				timestamp += timeIncrement
			}
		}
		_, err = fr.Seek(0, io.SeekStart)
		if err != nil {
			log.Fatal("Process=%d Failed to rewind file ptr, err=%v", processNo, err)
		}
	}

	if len(lines) > 0 {
		sendRequest(lines, url)
	}

	log.Infof("Process=%d Finished ingestion of Total Records %d", processNo, totalEvents)
}

func StartIngestion(totalEvents int, batchSize int, url string, indexSuffix string, filePrefix string, processCount int, timeRange int) {
	log.Println("Starting ingestion at ", url, "...")
	var wg sync.WaitGroup
	startTime := time.Now()
	totalEventsPerProcess := totalEvents / processCount
	timestamp := int(time.Now().Unix())
	timeIncrement := (timeRange * 1000) / totalEventsPerProcess
	eventIncrement := 1
	if timeIncrement <= 0 {
		eventIncrement = totalEventsPerProcess / (timeRange * 1000)
		timeIncrement = 1
	}
	for i := 0; i < processCount; i++ {
		fileName := fmt.Sprintf("%s-%d.json", filePrefix, i)
		wg.Add(1)
		go runIngestion(&wg, fileName, url, totalEventsPerProcess, batchSize, i+1, timestamp, timeIncrement, eventIncrement, indexSuffix)
	}
	wg.Wait()
	log.Println("Total logs ingested: ", totalEvents)
	totalTimeTaken := time.Since(startTime)
	log.Printf("Total Time Taken for ingestion %+v seconds", totalTimeTaken)
}
