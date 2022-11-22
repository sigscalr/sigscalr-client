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
}

var colf []string = []string{"iOS", "macOS", "windows", "android", "linux"}
var colfOptions uint32 = uint32(len(colf))

var colg []string = []string{"abc def", "ghi jkl", "mno pqr", "stu vwx", "yz"}
var colgOptions uint32 = uint32(len(colg))

var colh []string = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "ap-south-1", "eu-west-1", "me-south-1"}
var colhOptions uint32 = uint32(len(colh))

func generateRandomBody() map[string]interface{} {
	ev := make(map[string]interface{})
	ts := time.Now().Unix()

	randNum := fastrand.Uint32n(1_000)
	ev["a"] = fmt.Sprintf("batch-%d", randNum)
	ev["b"] = 8193
	ev["c"] = ts
	ev["e"] = "1103823372288"

	fIdx := fastrand.Uint32n(colfOptions)
	ev["f"] = colf[fIdx]

	gIdx := fastrand.Uint32n(colgOptions)
	ev["g"] = colg[gIdx]

	hIdx := fastrand.Uint32n(colhOptions)
	ev["h"] = colh[hIdx]
	ev["i"] = uuid.NewString()
	ev["j"] = fmt.Sprintf("S%d", ts%10)
	ev["k"] = "ethernet4Zone-test4"
	ev["l"] = fmt.Sprintf("group %d", ts%2)
	ev["m"] = "00000000000000000000ffff02020202"
	ev["n"] = "funccompanysaf3ti"
	ev["o"] = 6922966563614901991
	ev["p"] = "gtpv1-c"
	return ev
}

func (r *DynamicReader) Init(fName ...string) error {
	m := generateRandomBody()
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	return nil
}

func (r *DynamicReader) GetLogLine() ([]byte, error) {
	ll := generateRandomBody()
	return json.Marshal(ll)
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
