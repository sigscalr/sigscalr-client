package utils

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

type Reader interface {
	Init(fName string) error
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

func getMockBody() map[string]interface{} {
	ev := make(map[string]interface{})
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
	ev["dstuser"] = "funccompanysaf3ti"
	ev["seqno"] = 6922966563614901991
	ev["tunneled-app"] = "gtpv1-c"
	return ev
}

func (r *StaticReader) Init(fName string) error {
	m := getMockBody()
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

func (fr *FileReader) Init(fName string) error {
	fr.file = fName
	fr.filePosition = 0
	fr.logLines = make([][]byte, 0)
	fr.nextLogLines = make([][]byte, 0)
	fr.isChunkPrefetched = false
	fr.asyncPrefetch = false
	fr.editLock = &sync.Mutex{}
	if _, err := os.Stat(fName); errors.Is(err, os.ErrNotExist) {
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
