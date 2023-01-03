package utils

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastrand"
)

var json = jsoniter.ConfigFastest

type Reader interface {
	Init(fName ...string) error
	GetLogLine() ([]byte, error)
}

// file reader loads chunks from the file. Each request will get a sequential entry from the chunk.
// When the read index is close to the next chunk, pre-load next chunks
// Chunks should loop over the file multiple times if necessary
type FileReader struct {
	file    string
	lineNum int //number of lines read. lets us know where to start from

	editLock *sync.Mutex

	logLines [][]byte
	currIdx  int

	nextLogLines      [][]byte
	isChunkPrefetched bool
	asyncPrefetch     bool // is the chunk currenly being prefetched?
}

// Repeats the same log line each time
type StaticReader struct {
	logLine []byte
}

type DynamicReader struct {
	baseBody map[string]interface{}
}

var cold []string = []string{"iOS", "macOS", "windows", "android", "linux"}
var coldOptions uint32 = uint32(len(cold))

var cole []string = []string{"abc def", "ghi jkl", "mno pqr", "stu vwx", "yz"}
var coleOptions uint32 = uint32(len(cole))

var colf []string = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "ap-south-1", "eu-west-1", "me-south-1"}
var colfOptions uint32 = uint32(len(colf))

func generateRandomBody() map[string]interface{} {
	ev := make(map[string]interface{})

	randNum := fastrand.Uint32n(1_000)
	ev["batch"] = fmt.Sprintf("batch-%d", randNum)
	ev["traffic_flags"] = 8193
	ev["inbound_if"] = "1103823372288"
	ev["os"] = cold[fastrand.Uint32n(coldOptions)]
	ev["pod_name"] = cole[fastrand.Uint32n(coleOptions)]
	ev["region"] = colf[fastrand.Uint32n(colfOptions)]
	ev["ident"] = uuid.NewString()
	ev["dst_model"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
	ev["to"] = "ethernet4Zone-test4"
	ev["group"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
	ev["xff_ip"] = "00000000000000000000ffff02020202"
	ev["dstuser"] = "funccompanysaf3ti"
	ev["seqno"] = 6922966563614901991
	ev["tunneled"] = "gtpv1-c"
	ev["latency"] = fastrand.Uint32n(10_000_000)
	return ev
}

func (r *DynamicReader) Init(fName ...string) error {
	m := generateRandomBody()
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	r.baseBody = m
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	return nil
}

func (r *DynamicReader) GetLogLine() ([]byte, error) {
	err := r.randomizeDoc()
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(r.baseBody)
}

func (r *DynamicReader) randomizeDoc() error {
	r.baseBody["batch"] = fmt.Sprintf("batch-%d", fastrand.Uint32n(1_000))
	r.baseBody["os"] = cold[fastrand.Uint32n(coldOptions)]
	r.baseBody["pod_name"] = cole[fastrand.Uint32n(coleOptions)]
	r.baseBody["region"] = colf[fastrand.Uint32n(colfOptions)]
	r.baseBody["ident"] = uuid.NewString()
	r.baseBody["dst_model"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
	r.baseBody["group"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
	r.baseBody["latency"] = fastrand.Uint32n(10_000)
	return nil
}

func (r *StaticReader) Init(fName ...string) error {
	m := generateRandomBody()
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of event log line is %+v bytes", stringSize)
	r.logLine = body
	return nil
}

func (sr *StaticReader) GetLogLine() ([]byte, error) {
	return sr.logLine, nil
}

var chunkSize int = 10000

func (fr *FileReader) Init(fName ...string) error {
	fr.file = fName[0]
	fr.lineNum = 0
	fr.logLines = make([][]byte, 0)
	fr.nextLogLines = make([][]byte, 0)
	fr.isChunkPrefetched = false
	fr.asyncPrefetch = false
	fr.editLock = &sync.Mutex{}
	if _, err := os.Stat(fName[0]); errors.Is(err, os.ErrNotExist) {
		return err
	}
	err := fr.swapChunks()
	if err != nil {
		return err
	}
	return nil
}

func (fr *FileReader) GetLogLine() ([]byte, error) {
	fr.editLock.Lock()
	defer fr.editLock.Unlock()
	if fr.currIdx >= len(fr.logLines)-1 {
		err := fr.prefetchChunk(false)
		if err != nil {
			return []byte{}, err
		}
		err = fr.swapChunks()
		if err != nil {
			return []byte{}, err
		}
	}
	retVal := fr.logLines[fr.currIdx]
	fr.currIdx++
	if fr.currIdx > len(fr.logLines)/2 {
		go func() { _ = fr.prefetchChunk(false) }()
	}
	return retVal, nil
}

func (fr *FileReader) swapChunks() error {
	err := fr.prefetchChunk(false)
	if err != nil {
		return err
	}
	for fr.asyncPrefetch {
		time.Sleep(100 * time.Millisecond)
	}
	fr.logLines, fr.nextLogLines = fr.nextLogLines, fr.logLines
	fr.nextLogLines = make([][]byte, 0)
	fr.currIdx = 0
	fr.isChunkPrefetched = false
	return nil
}

// function will be called multiple times & will check if the next slice is already pre loaded
func (fr *FileReader) prefetchChunk(override bool) error {
	if fr.isChunkPrefetched {
		return nil
	}
	if fr.asyncPrefetch || override {
		return nil
	}
	fr.asyncPrefetch = true
	defer func() { fr.asyncPrefetch = false }()
	fd, err := os.Open(fr.file)
	if err != nil {
		log.Errorf("Failed to open file %s: %+v", fr.file, err)
		return err
	}
	defer fd.Close()
	_, err = fd.Seek(0, 0)
	if err != nil {
		return err
	}
	fileScanner := bufio.NewScanner(fd)
	tmpMap := make(map[string]interface{})
	lNum := 0
	for fileScanner.Scan() {
		if lNum <= fr.lineNum {
			lNum++
			continue
		}
		err := json.Unmarshal(fileScanner.Bytes(), &tmpMap)
		if err != nil {
			log.Errorf("Failed to unmarshal log entry %+v: lineNum %+v %+v", tmpMap, fr.lineNum, err)
			return err
		}
		logs, err := json.Marshal(tmpMap)
		if err != nil {
			log.Errorf("Failed to marshal log entry %+v: %+v", tmpMap, err)
			return err
		}
		fr.nextLogLines = append(fr.nextLogLines, logs)
		if len(fr.nextLogLines) > chunkSize {
			fr.isChunkPrefetched = true
			break
		}
		lNum++
	}

	if err := fileScanner.Err(); err != nil {
		log.Errorf("error in file scanner %+v", err)
		return err
	}
	fr.lineNum = lNum
	if len(fr.nextLogLines) <= chunkSize {
		// this will only happen if we reached the end of the file before filling the chunk
		fr.lineNum = 0
		return fr.prefetchChunk(true)
	}
	return nil
}
