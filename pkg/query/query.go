package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/montanaflynn/stats"

	log "github.com/sirupsen/logrus"
)

type queryTypes int

const (
	matchAll queryTypes = iota
)

func (q queryTypes) String() string {
	switch q {
	case matchAll:
		return "match all"
	default:
		return "UNKNOWN"
	}
}

func validateAndGetElapsedTime(esOutput map[string]interface{}, verbose bool) float64 {
	status, ok := esOutput["status"]
	if !ok {
		log.Fatalf("required key 'status' missing in response %+v", esOutput)
	}
	switch status := status.(type) {
	case float64:
		if status != float64(200) {
			log.Fatalf("non 200 status response by query: %v", status)
		}
	default:
		log.Fatalf("unknown type for 'status': %+T", status)
	}

	etime, ok := esOutput["took"]
	if !ok {
		log.Fatalf("required key 'took' missing in response %+v", esOutput)
	}
	if verbose {
		hits := esOutput["hits"]
		switch rawHits := hits.(type) {
		case map[string]interface{}:
			total := rawHits["total"]
			switch rawTotal := total.(type) {
			case map[string]interface{}:
				value := rawTotal["value"]
				relation := rawTotal["relation"]
				log.Infof("match all query: [%+v]ms. Hits: %+v %+v", etime, relation, value)
			case string:
				log.Infof("match all query: [%+v]ms. Hits: %+v %+v", etime, rawTotal)
			default:
				log.Fatalf("hits.total is not a map or string %+v", rawTotal)
			}
		default:
			log.Fatalf("hits is not a map[string]interface %+v", rawHits)
		}
	}
	return etime.(float64)
}

func getMatchAllQuery() []byte {
	time := time.Now().UnixMilli()
	time90d := time - (90 * 24 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match_all": true,
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time90d,
								"lte":    time,
								"format": "strict_date_optional_time",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// returns time returned by response payload
func sendMatchAllQuery(client *http.Client, url string, verbose bool) float64 {
	matchAll := getMatchAllQuery()
	req, err := http.NewRequest("POST", url, bytes.NewReader(matchAll))
	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	defer resp.Body.Close()
	rawBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(rawBody, &m)
	if err != nil {
		log.Fatalf("sendRequest: response unmarshal ERROR: %v", err)
	}
	return validateAndGetElapsedTime(m, verbose)
}

func initResultMap(numIterations int) map[queryTypes][]float64 {
	results := make(map[queryTypes][]float64)
	results[matchAll] = make([]float64, numIterations)
	return results
}

func logQuerySummary(numIterations int, res map[queryTypes][]float64) {
	log.Infof("-----Query Summary. Completed %d iterations----", numIterations)
	for qType, qRes := range res {
		p95, _ := stats.Percentile(qRes, 95)
		avg, _ := stats.Mean(qRes)
		max, _ := stats.Max(qRes)
		min, _ := stats.Min(qRes)
		log.Infof("QueryType: %s. Min:%+vms, Max:%+vms, Avg:%+vms, P95:%+vms", qType.String(), min, max, avg, p95)
	}
}

func StartQuery(dest string, numIterations int, prefix string, verbose bool) {
	client := http.DefaultClient
	if numIterations == 0 {
		log.Fatalf("Iterations must be greater than 0")
	}

	requestStr := fmt.Sprintf("%s/%s*/_search", dest, prefix)
	log.Infof("Using destination URL %+s", requestStr)
	results := initResultMap(numIterations)
	for i := 0; i < numIterations; i++ {
		time := sendMatchAllQuery(client, requestStr, verbose)
		results[matchAll][i] = time
	}

	logQuerySummary(numIterations, results)
}
