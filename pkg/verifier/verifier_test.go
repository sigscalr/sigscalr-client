package verifier

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

// cd pkg/verifier
// go test -run=Bench -bench=Benchmark_GenerateBody -cpuprofile cpuprofile.out -o rawsearch_cpu
// go tool pprof ./rawsearch_cpu cpuprofile.out
func Benchmark_GenerateBody(b *testing.B) {
	m := getMockBody()
	body, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Error marshalling mock body %+v", err)
	}
	for i := 0; i < 10_000; i++ {
		generateBulkBody(500, body, "test")
	}
}