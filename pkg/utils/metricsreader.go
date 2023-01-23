package utils

import (
	"fmt"
	"time"

	"github.com/valyala/fastrand"
)

type MetricsGenerator struct {
	nActiveTS uint32
}

func InitMetricsGenerator(activeTS int) *MetricsGenerator {
	return &MetricsGenerator{
		nActiveTS: uint32(activeTS),
	}
}

func (mg *MetricsGenerator) Init(fName ...string) error {
	return nil
}

func (mg *MetricsGenerator) GetLogLine() ([]byte, error) {
	return nil, fmt.Errorf("metrics generator can only be used with GetRawLog")
}

func (mg *MetricsGenerator) GetRawLog() (map[string]interface{}, error) {

	retVal := make(map[string]interface{})
	retVal["metric"] = "test.metric"
	retVal["timestamp"] = time.Now().Unix()
	retVal["value"] = fastrand.Uint32n(1_000)

	tags := make(map[string]interface{})
	tags["ident"] = fastrand.Uint32n(mg.nActiveTS)
	retVal["tags"] = tags

	return retVal, nil
}
