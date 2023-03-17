package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_metricsQuery(t *testing.T) {
	queries := []struct {
		dest          string
		numIterations int
		verbose       bool
		continuous    bool
	}{
		{
			dest:          "http://localhost:8081/otsdb",
			numIterations: 1,
			verbose:       false,
			continuous:    false,
		},
	}
	for _, testQ := range queries {
		validResults := StartMetricsQuery(testQ.dest, testQ.numIterations, testQ.continuous, testQ.verbose)
		for _, v := range validResults {
			assert.Equal(t, true, v)
		}
	}
}
