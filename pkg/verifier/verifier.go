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

func updateMockBody(ev map[string]interface{}) {
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
	ev["dstuser"] = "funccompany\\saf3ti"
	ev["seqno"] = 6922966563614901991
	ev["tunneled-app"] = "gtpv1-c"
	ev["session_end_reason"] = 0
	ev["src_vendor"] = "Samsung"
	ev["dst_osversion"] = "Android v9"
	ev["hostid"] = "4040404040"
	ev["vsys"] = "vsys1"
	ev["bytes"] = 948069
	ev["subtype"] = 2
	ev["dst_host"] = "sam-121"
	ev["subcategory-of-app"] = "infrastructure"
	ev["vsys_id"] = 1
	ev["s_decrypted"] = 1
	ev["actionflags"] = 922337036854775808
	ev["session_owner"] = "session_owner-2"
	ev["chunks"] = 134
	ev["sport"] = 9256
	ev["is-saas-of-app"] = 0
	ev["category"] = "0"
	ev["bytes_received"] = 629169
	ev["dst"] = "fe806211123456786202b3fffe1e8329"
	ev["src_model"] = "720P/60"
	ev["srcloc"] = "west-coast"
	ev["natsport"] = 25810
	ev["parent_session_id"] = 8815
	ev["dst_mac"] = "180872328842"
	ev["dst_vendor"] = "Samsung"
	ev["bpadding"] = 0
	ev["link_change_count"] = 0
	ev["proxy"] = 1
	ev["src"] = "fe8055eeee89abcde202b3fffe1e8329"
	ev["sesscache_l7_done"] = 1
	ev["sanctioned-state-of-app"] = 0
	ev["fwd"] = 1
	ev["bytes_sent"] = 318900
	ev["chunks_received"] = 67
	ev["dynusergroup_name"] = "test-rtyug-5"
	ev["action"] = 0
	ev["repeatcnt"] = 1
	ev["natsrc"] = "00000000000000000000ffff4171280c"
	ev["app"] = "gtpv1-c"
	ev["characteristic-of-app"] = "pervasive-use"
	ev["pkts_sent"] = 704
	ev["pod_namespace"] = "myns_default"
	ev["non_std_dport"] = 1
	ev["dst_category"] = "S-Phone"
	ev["decrypt_mirror"] = 1
	ev["action_source"] = 0
	ev["from"] = "untrust"
	ev["technology-of-app"] = "network-protocol"
	ev["dst_profile"] = "s-profile"
	ev["assoc_id"] = 1369094286720630776
	ev["start"] = 1612484370
	ev["type"] = 0
	ev["outbound_if"] = "1103823372288"
	ev["tunnelid_imsi"] = 34
	ev["sessionid"] = 646791
	ev["src_osfamily"] = "M4500"
	ev["dst_osfamily"] = "Galaxy"
	ev["category-of-app"] = "networking"
	ev["tunnel"] = 5
	ev["non-standard-dport"] = 52
	ev["src_profile"] = "s-profile"
	ev["http2_connection"] = 0
	ev["container-of-app"] = "gtp"
	ev["serialnumber"] = "HW0000001"
	ev["risk-of-app"] = 1
	ev["serial"] = "LSCHMID128"
	ev["proto"] = 6
	ev["src_category"] = "S-Phone"
	ev["nssai_sd"] = 29766
	ev["receive_time"] = 1612484416
	ev["natdst"] = "00000000000000000000ffffc0a85087"
	ev["nssai_sst"] = 196
	ev["src_mac"] = "264989591511"
	ev["time_received"] = 1612484413
	ev["src_osversion"] = "Android v8"
	ev["rule_uuid"] = "a1d8e236-d731-4913-9a61-6824ae1bfadb"
	ev["container_id"] = "1873cc5c-0d31"
	ev["pkts_received"] = 1714
	ev["elapsed"] = 19
	ev["packets"] = 2418
	ev["rule"] = "deny-attackers"
	ev["device_name"] = "henry-NH-VM-128"
	ev["flags"] = 17826816
	ev["s_encrypted"] = 1
	ev["dport"] = 11060
	ev["src_host"] = "sam-123"
}

func generateBulkBody(recs int, idx string) string {
	actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	m := make(map[string]interface{}, numCols)
	for i := 0; i < recs; i++ {
		bb.WriteString(actionLine)
		updateMockBody(m)
		retVal, err := json.Marshal(m)
		if err != nil {
			log.Fatalf("Error marshalling mock body %+v", err)
		}
		bb.Write(retVal)
	}
	retVal := bb.String()
	return retVal
}

func runIngestion(wg *sync.WaitGroup, url string, totalEvents, batchSize, processNo int, indexSuffix string, ctr *uint64) {
	defer wg.Done()
	eventCounter := 0
	indexName := fmt.Sprintf("%v", indexSuffix)
	for eventCounter < totalEvents {

		recsInBatch := batchSize
		if eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		payload := generateBulkBody(recsInBatch, indexName)
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

	lastPrintedCount := float64(0)
readChannel:
	for {
		select {
		case <-done:
			break readChannel
		case <-ticker.C:
			totalTimeTaken := time.Since(startTime)
			eSent := float64(totalSent)
			eventsPerMin := (eSent - lastPrintedCount)
			log.Infof("Total elapsed time: %+v. Total sent events %+v. Events per minute %+v", totalTimeTaken, totalSent, eventsPerMin)
			lastPrintedCount = eSent
		}
	}
	log.Println("Total logs ingested: ", totalEvents)
	totalTimeTaken := time.Since(startTime)
	log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
}
