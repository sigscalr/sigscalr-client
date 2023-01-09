package trace

import (
	"time"

	"github.com/google/uuid"
	"github.com/valyala/fastrand"

	log "github.com/sirupsen/logrus"
)

func StartTraceGeneration(file string, numTraces int, maxSpans int) {
	log.Println("Starting generation of ", numTraces, " traces...")
	traceCounter := 0
	for traceCounter < numTraces {
		var traceId = uuid.NewString()
		var randomNumberOfSpans = fastrand.Uint32n(uint32(maxSpans))
		spanCounter := 0
		for spanCounter < int(randomNumberOfSpans) {
			generateChildSpan(traceId)
		}
		traceCounter += 1
	}
}

func unint32(maxSpans int) {
	panic("unimplemented")
}

func generateChildSpan(traceId string) map[string]interface{} {
	span := make(map[string]interface{})

	span["spanID"] = uuid.NewString()
	span["startTime"] = time.Now()
	span["startTimeMillis"] = time.Now().UnixNano() / int64(time.Millisecond)
	span["flags"] = 1
	span["operationName"] = " HTTP GET"
	span["duration"] = fastrand.Uint32n(1_000)
	span["tags"] = generateTagBody()
	return span
}

func generateTagBody() map[string]interface{} {
	tags := make(map[string]interface{})
	return tags
}
