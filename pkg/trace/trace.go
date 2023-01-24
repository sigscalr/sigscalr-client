package trace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/valyala/fastrand"

	log "github.com/sirupsen/logrus"
)

var envs []string = []string{"prod", "dev"}
var ar_dc []string = []string{"LON", "PAR", "DEN", "PHX", "DFW", "ATL", "BOS", "MHT", "JFK", "CAR", "LHR", "AMS", "FRA", "BOM", "CAL", "DEL", "MAD", "CHE", "FGH", "RTY", "UIO", "MHJ", "HAN", "YHT", "YUL", "MOL", "FOS", "KUN", "SRI", "FGR", "SUN", "PRI", "TAR", "SAR", "ADI", "ERT", "ITR", "DOW", "UQW", "QBF", "POK", "HQZ", "ZAS", "POK", "LIP", "UYQ", "OIK", "TRU", "POL", "NMC", "AZQ"}

func StartTraceGeneration(file string, numTraces int, maxSpans int) {
	log.Println("Starting generation of ", numTraces, " traces...")
	traceCounter := 0
	log.Println("Opening file ", file, "...")
	f, err := os.Create(file)
	w := bufio.NewWriter(f)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for traceCounter < numTraces {
		var traceId = uuid.NewString()
		var randomNumberOfSpans = 1 + fastrand.Uint32n(uint32(maxSpans))
		spanCounter := 0
		for spanCounter < int(randomNumberOfSpans) {
			span := generateChildSpan(traceId, spanCounter)
			span["traceID"] = traceId
			bytes, _ := json.Marshal(span)
			_, err1 := w.Write(bytes)
			if err1 != nil {
				log.Fatal(err1)
			}
			_, err2 := w.WriteString("\n")
			if err2 != nil {
				log.Fatal(err2)
			}
			spanCounter += 1
		}
		traceCounter += 1
	}
	w.Flush()
	f.Close()
}

func generateChildSpan(traceId string, spanCounter int) map[string]interface{} {
	span := make(map[string]interface{})
	spanId := uuid.NewString()
	span["spanID"] = spanId
	span["startTime"] = time.Now()
	span["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	span["flags"] = 1
	span["operationName"] = " HTTP GET"
	span["duration"] = fastrand.Uint32n(1_000)
	span["tags"] = generateTagBody(10)
	span["process"] = generateProcessTagsBody(10)
	span["references"] = generateReferenceBody(traceId, spanId, spanCounter)
	return span
}

func generateReferenceBody(traceId string, spanId string, spanCounter int) map[string]interface{} {
	references := make(map[string]interface{})
	if spanCounter == 0 {
		references["refType"] = "CHILD_OF"
	} else {
		references["refType"] = "FollowsFrom"
	}
	references["traceID"] = traceId
	references["spanID"] = spanId
	return references
}

func generateProcessTagsBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, 3)
	tagsCounter := 0
	for tagsCounter < 3 {
		tags := make(map[string]interface{})
		randomNodeId := fastrand.Uint32n(2_000)
		randomPodId := fastrand.Uint32n(20_000)
		randomUserId := fastrand.Uint32n(2_000_000)
		tags["key"] = "node_id"
		tags["value"] = fmt.Sprintf("node-%d", randomNodeId)
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
		tags = make(map[string]interface{})
		tags["key"] = "pod_id"
		tags["value"] = fmt.Sprintf("pod-%d", randomPodId)
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
		tags = make(map[string]interface{})
		tags["key"] = "user_id"
		tags["value"] = fmt.Sprintf("user-%d", randomUserId)
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
	}
	return listOfTags
}

func generateTagBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, n*4)
	tagsCounter := 0
	for tagsCounter < 4*n {
		tags := make(map[string]interface{})
		randNumrequest := fastrand.Uint32n(2_000_000_0)
		randNumCluster := fastrand.Uint32n(2_000)
		randNumEnv := fastrand.Uint32n(2)
		randNumDc := fastrand.Uint32n(uint32(len(ar_dc)))
		tags["key"] = "cluster"
		tags["value"] = fmt.Sprintf("cluster-%d", randNumCluster)
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
		tags = make(map[string]interface{})
		tags["key"] = "env"
		tags["value"] = envs[randNumEnv]
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
		tags = make(map[string]interface{})
		tags["key"] = "dc"
		tags["value"] = ar_dc[randNumDc]
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
		tags = make(map[string]interface{})
		tags["key"] = "request_id"
		tags["value"] = fmt.Sprintf("request-%d", randNumrequest)
		tags["type"] = "string"
		listOfTags[tagsCounter] = tags
		tagsCounter++
	}
	return listOfTags
}
