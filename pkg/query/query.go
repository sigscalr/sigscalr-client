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
	matchMultiple
	matchRange
	needleInHaystack
	keyValueQuery
	freeText
)

func (q queryTypes) String() string {
	switch q {
	case matchAll:
		return "match all"
	case matchMultiple:
		return "match multiple"
	case matchRange:
		return "match range"
	case needleInHaystack:
		return "needle in haystack"
	case keyValueQuery:
		return "single key=value"
	case freeText:
		return "free text"
	default:
		return "UNKNOWN"
	}
}

func validateAndGetElapsedTime(qType queryTypes, esOutput map[string]interface{}, verbose bool) float64 {

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
				log.Infof("%s query: [%+v]ms. Hits: %+v %+v", qType.String(), etime, relation, value)
			case string:
				log.Infof("%s query: [%+v]ms. Hits: %+v", qType.String(), etime, rawTotal)
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
	time1hr := time - (1 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1hr,
								"lte":    time,
								"format": "epoch_millis",
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

// job_title=<<random_title>> AND user_color=<<random_color>> AND j != "group 0"
func getMatchMultipleQuery() []byte {
	time := time.Now().UnixMilli()
	time2hr := time - (2 * 24 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"job_title": "Engineer",
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"job_description": "Senior",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time2hr,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"group": "group 0",
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

// 10 <= latency <= 30
func getRangeQuery() []byte {
	time := time.Now().UnixMilli()
	time1d := time - (1 * 24 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"latency": map[string]interface{}{
								"gte": 10,
								"lte": 8925969,
							},
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1d,
								"lte":    time,
								"format": "epoch_millis",
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

// matches a different uuid each query. This will likely have 0 hits
func getNeedleInHaystackQuery() []byte {
	time := time.Now().UnixMilli()
	time90d := time - (90 * 24 * 60 * 60 * 1000)

	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": fmt.Sprintf("ident:%s", "ffa4c7d4-5f21-457b-89ea-b5ad29968510"),
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time90d,
								"lte":    time,
								"format": "epoch_millis",
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

// matches a simple key=value using query_string
func getSimpleFilter() []byte {
	time := time.Now().UnixMilli()
	time6hr := time - (6 * 24 * 60 * 60 * 1000)

	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": "state:California",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time6hr,
								"lte":    time,
								"format": "epoch_millis",
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

// free text search query for a job title
func getFreeTextSearch() []byte {
	time := time.Now().UnixMilli()
	time1hr := time - (1 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": "Representative",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1hr,
								"lte":    time,
								"format": "epoch_millis",
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

func sendSingleRequest(qType queryTypes, client *http.Client, body []byte, url string, verbose bool) float64 {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
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
	return validateAndGetElapsedTime(qType, m, verbose)
}

func initResultMap(numIterations int) map[queryTypes][]float64 {
	results := make(map[queryTypes][]float64)
	results[matchAll] = make([]float64, numIterations)
	results[matchMultiple] = make([]float64, numIterations)
	results[matchRange] = make([]float64, numIterations)
	results[needleInHaystack] = make([]float64, numIterations)
	results[keyValueQuery] = make([]float64, numIterations)
	results[freeText] = make([]float64, numIterations)
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

func StartQuery(dest string, numIterations int, prefix string, continuous, verbose bool, queryType string) {
	client := http.DefaultClient
	if numIterations == 0 && !continuous {
		log.Fatalf("Iterations must be greater than 0")
	}

	requestStr := fmt.Sprintf("%s/%s*/_search", dest, prefix)

	log.Infof("Using destination URL %+s", requestStr)

	var continuousFlag int
	if continuous {
		continuousFlag = 1
	}

	switch queryType {
	case "playground":
		runPlaygroundQueries(client, requestStr, numIterations, continuousFlag)
		return
	case "":
	default:
		log.Fatalf("unsupported query")
	}

	var results map[queryTypes][]float64

	if !continuous {
		results = initResultMap(numIterations)
	} else {
		runContinuousQueries(client, requestStr)
	}

	for i := 0; i < numIterations; i++ {
		rawMatchAll := getMatchAllQuery()
		time := sendSingleRequest(matchAll, client, rawMatchAll, requestStr, verbose)
		results[matchAll][i] = time

		rawMultiple := getMatchMultipleQuery()
		time = sendSingleRequest(matchMultiple, client, rawMultiple, requestStr, verbose)
		results[matchMultiple][i] = time

		rawRange := getRangeQuery()
		time = sendSingleRequest(matchRange, client, rawRange, requestStr, verbose)
		results[matchRange][i] = time

		rawNeeldQuery := getNeedleInHaystackQuery()
		time = sendSingleRequest(needleInHaystack, client, rawNeeldQuery, requestStr, verbose)
		results[needleInHaystack][i] = time

		sQuery := getSimpleFilter()
		time = sendSingleRequest(keyValueQuery, client, sQuery, requestStr, verbose)
		results[keyValueQuery][i] = time

		fQuery := getFreeTextSearch()
		time = sendSingleRequest(freeText, client, fQuery, requestStr, verbose)
		results[freeText][i] = time
	}

	if !continuous {
		logQuerySummary(numIterations, results)
	}
}

// this will never save time statistics per query and will always log results
func runContinuousQueries(client *http.Client, requestStr string) {
	for {
		rawMatchAll := getMatchAllQuery()
		_ = sendSingleRequest(matchAll, client, rawMatchAll, requestStr, true)

		rawMultiple := getMatchMultipleQuery()
		_ = sendSingleRequest(matchMultiple, client, rawMultiple, requestStr, true)

		rawRange := getRangeQuery()
		_ = sendSingleRequest(matchRange, client, rawRange, requestStr, true)

		sQuery := getSimpleFilter()
		_ = sendSingleRequest(keyValueQuery, client, sQuery, requestStr, true)

		fQuery := getFreeTextSearch()
		_ = sendSingleRequest(freeText, client, fQuery, requestStr, true)
	}
}

// this will run queries specific to the playground setup we have.
func runPlaygroundQueries(client *http.Client, requestStr string, numIterations int, continuousFlag int) {

	var results map[queryTypes][]float64

	if continuousFlag == 0 {
		results = initResultMap(numIterations)
	}

	for i := 0; i < numIterations; i++ {
		times := make([]float64, 5)

		rawMatchAll := getMatchAllQuery()
		times = append(times, sendSingleRequest(matchAll, client, rawMatchAll, requestStr, false))
		// if continuousFlag == 0 {
		// results[matchAll][i] =
		//}

		rawMultiple := getMatchMultipleQuery()
		times = append(times, sendSingleRequest(matchMultiple, client, rawMultiple, requestStr, true))
		// if continuousFlag == 0 {
		// results[match][i] =
		//}

		rawRange := getRangeQuery()
		times = append(times, sendSingleRequest(matchRange, client, rawRange, requestStr, true))

		sQuery := getSimpleFilter()
		times = append(times, sendSingleRequest(keyValueQuery, client, sQuery, requestStr, true))

		fQuery := getFreeTextSearch()
		times = append(times, sendSingleRequest(freeText, client, fQuery, requestStr, true))

		if continuousFlag == 0 {
			results[matchAll][i] = times[0]
			results[matchMultiple][i] = times[1]
			results[matchRange][i] = times[2]
			results[keyValueQuery][i] = times[3]
			results[freeText][i] = times[4]
		}
		i = i - continuousFlag
	}
	if continuousFlag == 0 {
		logQuerySummary(numIterations, results)
	}
}
