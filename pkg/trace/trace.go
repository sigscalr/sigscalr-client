package trace

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/valyala/fastrand"

	log "github.com/sirupsen/logrus"
)

var envs []string = []string{"prod", "dev"}
var ar_dc []string = []string{"LON", "PAR", "DEN", "PHX", "DFW", "ATL", "BOS", "MHT", "JFK", "CAR", "LHR", "AMS", "FRA", "BOM", "CAL", "DEL", "MAD", "CHE", "FGH", "RTY", "UIO", "MHJ", "HAN", "YHT", "YUL", "MOL", "FOS", "KUN", "SRI", "FGR", "SUN", "PRI", "TAR", "SAR", "ADI", "ERT", "ITR", "DOW", "UQW", "QBF", "POK", "HQZ", "ZAS", "POK", "LIP", "UYQ", "OIK", "TRU", "POL", "NMC", "AZQ"}

func randomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes)
}

func StartTraceGeneration(file string, numTraces int, maxSpans int) {
	log.Println("Starting generation of ", numTraces, " traces...")
	traceCounter := 0
	log.Println("Opening file ", file, "...")
	log.Println("Opening file trace_service.json...")
	service_file, err := os.Create("trace_service.json")
	service_writer := bufio.NewWriter(service_file)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(file)
	w := bufio.NewWriter(f)
	if err != nil {
		log.Fatal(err)
	}
	defer service_file.Close()
	defer f.Close()
	for traceCounter < numTraces {
		var traceId = randomHex(8)
		var randomNumberOfSpans = 1 + fastrand.Uint32n(uint32(maxSpans))
		var prevSpanId = randomHex(8)
		traceRoot := generateRootTrace(traceId)
		WriteToFile(traceRoot,w)
		serviceAndOperationJson := getServiceAndOperationObject(traceRoot["operationName"], traceRoot["process"])
		WriteToServiceFile(serviceAndOperationJson, service_writer)
		spanCounter := 0
		for spanCounter < int(randomNumberOfSpans) {
			span,spanId := generateChildSpan(traceId, spanCounter,prevSpanId)
			serviceAndOperationJson := getServiceAndOperationObject(span["operationName"], span["process"])
			span["traceID"] = traceId
			WriteToFile(span, w)
			WriteToServiceFile(serviceAndOperationJson, service_writer)
			spanCounter += 1
			prevSpanId = spanId
		}
		traceCounter += 1
	}
	w.Flush()
	service_writer.Flush()
	service_file.Close()
	f.Close()
}

func generateRootTrace(traceId string) map[string]interface{} {
	rootTrace := make(map[string]interface{})
	rootTrace["spanID"] = traceId
	rootTrace["startTime"] = time.Now().UnixNano() / int64(time.Microsecond)
	rootTrace["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	rootTrace["flags"] = 1
	rootTrace["operationName"] = "HTTP GET"
	rootTrace["duration"] = fastrand.Uint32n(1_000)
	rootTrace["tags"] = generateTagBody(10)
	rootTrace["process"] = generateProcessTagsBody(10)
	rootTrace["references"] = make([]map[string]interface{},0)
	return rootTrace
}

func WriteToServiceFile(serviceAndOperationJson map[string]string, w *bufio.Writer) {
	bytes, _ := json.Marshal(serviceAndOperationJson)
	_, err1 := w.Write(bytes)
	if err1 != nil {
		log.Fatal(err1)
	}
	_, err2 := w.WriteString("\n")
	if err2 != nil {
		log.Fatal(err2)
	}
}

func WriteToFile(span map[string]interface{}, w *bufio.Writer) {
	bytes, _ := json.Marshal(span)
	_, err1 := w.Write(bytes)
	if err1 != nil {
		log.Fatal(err1)
	}
	_, err2 := w.WriteString("\n")
	if err2 != nil {
		log.Fatal(err2)
	}
}

func getServiceAndOperationObject(operationName, processTags interface{}) map[string]string {
	serviceAndOperationObject := make(map[string]string)
	if opName, ok := operationName.(string); ok {
		serviceAndOperationObject["operationName"] = opName
	}
	if serviceName, ok := processTags.(map[string]interface{})["serviceName"].(string); ok {
		serviceAndOperationObject["serviceName"] = serviceName
	}
	return serviceAndOperationObject
}

func generateChildSpan(traceId string, spanCounter int,prevSpanId string) (map[string]interface{},string) {
	span := make(map[string]interface{})
	spanId := randomHex(8)
	span["spanID"] = spanId
	span["startTime"] = time.Now().UnixNano() / int64(time.Microsecond)
	span["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	span["flags"] = 1
	span["operationName"] = "HTTP GET"
	span["duration"] = fastrand.Uint32n(1_000)
	span["tags"] = generateTagBody(10)
	span["process"] = generateProcessTagsBody(10)
	span["references"] = generateReferenceBody(traceId, spanId, spanCounter,prevSpanId)
	return span,spanId
}

func generateReferenceBody(traceId string, spanId string, spanCounter int,prevSpanId string) []map[string]interface{} {
	references := make([]map[string]interface{}, 1)
	reference := make(map[string]interface{})
	if spanCounter == 0 {
		reference["refType"] = "CHILD_OF"
		reference["spanID"] = traceId
	} else {
		reference["refType"] = "FOLLOWS_FROM"
		reference["spanID"] = prevSpanId
	}
	reference["traceID"] = traceId
	references[0] = reference
	return references
}

func generateProcessTagsBody(n int) map[string]interface{} {
	processBody := make(map[string]interface{})
	randomServiceId := fastrand.Uint32n(10)
	processBody["serviceName"] = fmt.Sprintf("service-%d", randomServiceId)
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
	processBody["tags"] = listOfTags
	return processBody
}

func generateTagBody(n int) []map[string]interface{} {
	listOfTags := make([]map[string]interface{}, 4)
	tagsCounter := 0
	for tagsCounter < 4 {
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
