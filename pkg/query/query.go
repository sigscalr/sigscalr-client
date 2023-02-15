package query

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/fasthttp/websocket"
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

func StartQuery(dest string, numIterations int, prefix string, continuous, verbose bool) {
	client := http.DefaultClient
	if numIterations == 0 && !continuous {
		log.Fatalf("Iterations must be greater than 0")
	}

	requestStr := fmt.Sprintf("%s/%s*/_search", dest, prefix)

	log.Infof("Using destination URL %+s", requestStr)
	if continuous {
		runContinuousQueries(client, requestStr)
	}

	results := initResultMap(numIterations)
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

	logQuerySummary(numIterations, results)
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

// Run queries from a csv file

func RunQueryFromFile(dest string, numIterations int, prefix string, continuous, verbose bool, filepath string) {
	// open file
	f, err := os.Open(filepath)
	if err != nil {
		log.Errorf("RunQueryFromFile: Error in opening file: %v, err: %v", filepath, err)
		return
	}

	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("RunQueryFromFile: Error in reading file: %v, err: %v", filepath, err)
			return
		}

		queryString, _ := url.Parse(rec[0])
		queryParams, _ := url.ParseQuery(queryString.RawQuery)

		data := map[string]interface{}{
			"state":      "query",
			"searchText": queryParams["searchText"][0],
			"startEpoch": queryParams["startEpoch"][0],
			"endEpoch":   queryParams["endEpoch"][0],
			"indexName":  queryParams["indexName"][0],
		}

		// create websocket connection
		conn, _, err := websocket.DefaultDialer.Dial("ws://localhost/api/search/ws", nil)
		if err != nil {
			log.Fatal("RunQueryFromFile: Error connecting to WebSocket server: ", err)
			return
		}
		defer conn.Close()

		err = conn.WriteJSON(data)
		if err != nil {
			log.Errorf("Received err message from server: %+v\n", err)
			break
		}

		readEvent := make(map[string]interface{})
		for {
			err = conn.ReadJSON(&readEvent)
			if err != nil {
				log.Infof("Received error from server: %+v\n", err)
				break
			}
			switch readEvent["state"] {
			case "RUNNING":

			case "QUERY_UPDATE":

			case "COMPLETE":
				for eKey, eValue := range readEvent {
					if eKey == "totalMatched" {
						for k, v := range eValue.(map[string]interface{}) {
							if k == "value" {
								actualHits := strconv.FormatInt(int64(v.(float64)), 10)
								if actualHits != rec[1] {
									log.Fatalf("RunQueryFromFile: Actual Hits: %v does not match Expected hits: %v for query:%v", actualHits, rec[1], rec[0])
								} else {
									log.Infof("RunQueryFromFile: Query %v was succesful", rec[0])
								}
							}
						}
					}
				}
			default:
				log.Infof("Received unknown message from server: %+v\n", readEvent)
			}
		}
	}
}
