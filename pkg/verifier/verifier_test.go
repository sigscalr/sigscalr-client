package verifier

import "testing"

// cd pkg/verifier
// go test -run=Bench -bench=Benchmark_GenerateBody -cpuprofile cpuprofile.out -o rawsearch_cpu
// go tool pprof ./rawsearch_cpu cpuprofile.out
func Benchmark_GenerateBody(b *testing.B) {
	for i := 0; i < 10_000; i++ {
		generateBulkBody(500, "test")
	}
}
