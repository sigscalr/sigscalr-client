package trace

import (
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
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for traceCounter < numTraces {
		var traceId = uuid.NewString()
		var randomNumberOfSpans = 1 + fastrand.Uint32n(uint32(maxSpans))
		spanCounter := 0
		for spanCounter < int(randomNumberOfSpans) {
			span := generateChildSpan(traceId)
			span["traceID"] = traceId
			bytes, _ := json.Marshal(span)
			_, err2 := f.Write(bytes)
			if err2 != nil {
				log.Fatal(err2)
			}
			spanCounter += 1
		}
		traceCounter += 1
	}
	f.Close()
}

func generateChildSpan(traceId string) map[string]interface{} {
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
	span["references"] = generateReferenceBody(traceId, spanId)
	return span
}

func generateReferenceBody(traceId string, spanId string) map[string]interface{} {
	references := make(map[string]interface{})
	references["traceID"] = traceId
	references["spanID"] = spanId
	return references
}

func generateProcessTagsBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, n)
	tagsCounter := 0
	for tagsCounter < n {
		tags := make(map[string]interface{})
		randomNodeId := fastrand.Uint32n(2_000)
		randomPodId := fastrand.Uint32n(20_000)
		randomUserId := fastrand.Uint32n(2_000_000)
		tags["node_id"] = fmt.Sprintf("node-%d", randomNodeId)
		tags["pod_id"] = fmt.Sprintf("pod-%d", randomPodId)
		tags["user_id"] = fmt.Sprintf("user-%d", randomUserId)
		listOfTags[tagsCounter] = tags
		tagsCounter += 1
	}
	return listOfTags
}

func generateTagBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, n)
	tagsCounter := 0
	for tagsCounter < n {
		tags := make(map[string]interface{})
		randNumrequest := fastrand.Uint32n(2_000_000_0)
		randNumCluster := fastrand.Uint32n(2_000)
		randNumEnv := fastrand.Uint32n(2)
		randNumDc := fastrand.Uint32n(uint32(len(ar_dc)))
		tags["cluster"] = fmt.Sprintf("cluster-%d", randNumCluster)
		tags["env"] = fmt.Sprintf("%s", envs[randNumEnv])
		tags["dc"] = fmt.Sprintf("%s", ar_dc[randNumDc])
		tags["request_id"] = fmt.Sprintf("request-%d", randNumrequest)
		listOfTags[tagsCounter] = tags
		tagsCounter += 1
	}
	return listOfTags
}
