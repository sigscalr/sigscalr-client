package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
	file         string
	filePosition int64 // offset of last read line in file

	editLock *sync.Mutex

	logLines [][]byte
	currIdx  int

	nextLogLines      [][]byte
	isChunkPrefetched bool
	asyncPrefetch     bool // is the chunk currenly being prefetched?
}

type readCounter struct {
	io.Reader
	BytesRead int
}

func (r *readCounter) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.BytesRead += n
	return n, err
}

// Repeats the same log line each time
type StaticReader struct {
	logLine []byte
}

type DynamicReader struct {
	baseBody map[string]interface{}
	body     []byte
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
	ev["a"] = fmt.Sprintf("batch-%d", randNum)
	ev["b"] = 8193
	ev["c"] = "1103823372288"
	ev["d"] = cold[fastrand.Uint32n(coldOptions)]
	ev["e"] = cole[fastrand.Uint32n(coleOptions)]
	ev["f"] = colf[fastrand.Uint32n(colfOptions)]
	ev["g"] = uuid.NewString()
	ev["h"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
	ev["i"] = "ethernet4Zone-test4"
	ev["j"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
	ev["k"] = "00000000000000000000ffff02020202"
	ev["l"] = "funccompanysaf3ti"
	ev["m"] = 6922966563614901991
	ev["n"] = "gtpv1-c"
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
	r.body = body
	return nil
}

func (r *DynamicReader) GetLogLine() ([]byte, error) {
	err := r.randomizeDoc()
	if err != nil {
		return []byte{}, err
	}
	return r.body, nil
}

func (r *DynamicReader) randomizeDoc() error {
	r.baseBody["a"] = fmt.Sprintf("batch-%d", fastrand.Uint32n(1_000))
	r.baseBody["d"] = cold[fastrand.Uint32n(coldOptions)]
	r.baseBody["e"] = cole[fastrand.Uint32n(coleOptions)]
	r.baseBody["f"] = colf[fastrand.Uint32n(colfOptions)]
	r.baseBody["g"] = uuid.NewString()
	r.baseBody["h"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
	r.baseBody["j"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
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

var chunkSize int = 100_000

func (fr *FileReader) Init(fName ...string) error {
	fr.file = fName[0]
	fr.filePosition = 0
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
		go fr.prefetchChunk(false)
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
	fd.Seek(fr.filePosition, 0)
	b := &readCounter{Reader: fd}
	fileScanner := bufio.NewScanner(b)
	tmpMap := make(map[string]interface{})
	ctr := 0
	for fileScanner.Scan() {
		ctr++
		json.Unmarshal(fileScanner.Bytes(), &tmpMap)
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
	}

	if err := fileScanner.Err(); err != nil {
		log.Errorf("error in file scanner %+v", err)
		return err
	}
	fr.filePosition += int64(b.BytesRead)
	if len(fr.nextLogLines) <= chunkSize {
		// this will only happen if we reached the end of the file before filling the chunk
		fr.filePosition = 0
		return fr.prefetchChunk(true)
	}
	return nil
}
