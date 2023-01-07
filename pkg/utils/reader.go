package utils

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/brianvoe/gofakeit/v6"
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
	baseBody  map[string]interface{}
	tNowEpoch uint64
	faker     *gofakeit.Faker
}

func randomizeBody(f *gofakeit.Faker, m map[string]interface{}, ts uint64) {
	randNum := fastrand.Uint32n(1_000)
	// sentenceLen := int(fastrand.Uint32n(25))
	m["batch"] = fmt.Sprintf("batch-%d", randNum)
	p := f.Person()
	m["first_name"] = p.FirstName
	m["last_name"] = p.LastName
	m["gender"] = p.Gender
	m["ssn"] = p.SSN
	m["image"] = p.Image
	m["hobby"] = p.Hobby

	m["job_description"] = p.Job.Descriptor
	m["job_level"] = p.Job.Level
	m["job_title"] = p.Job.Title
	m["job_company"] = p.Job.Company

	m["address"] = p.Address.Address
	m["street"] = p.Address.Street
	m["city"] = p.Address.City
	m["state"] = p.Address.State
	m["zip"] = p.Address.Zip
	m["country"] = p.Address.Country
	m["latitude"] = p.Address.Latitude
	m["longitude"] = p.Address.Longitude
	m["user_phone"] = p.Contact.Phone
	m["user_email"] = p.Contact.Email

	m["user_color"] = f.Color()
	m["app_name"] = f.AppName()
	m["app_version"] = f.AppVersion()
	m["ident"] = f.UUID()
	m["user_agent"] = f.UserAgent()
	m["url"] = f.URL()
	m["group"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
	m["question"] = f.Question()
	m["latency"] = fastrand.Uint32n(10_000_000)
	m["timestamp"] = ts
}

func (r *DynamicReader) generateRandomBody() {
	randomizeBody(r.faker, r.baseBody, r.tNowEpoch)
	r.tNowEpoch -= 2
}

func (r *DynamicReader) Init(fName ...string) error {
	r.faker = gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000)))
	r.baseBody = make(map[string]interface{})
	r.tNowEpoch = uint64(time.Now().UnixMilli() - 10_000_000)
	r.generateRandomBody()
	body, err := json.Marshal(r.baseBody)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	r.tNowEpoch = uint64(time.Now().UnixMilli()) - 80*24*3600*1000
	return nil
}

func (r *DynamicReader) GetLogLine() ([]byte, error) {
	r.generateRandomBody()
	return json.Marshal(r.baseBody)
}

func (r *StaticReader) Init(fName ...string) error {
	m := make(map[string]interface{})
	f := gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000)))
	randomizeBody(f, m, uint64(time.Now().UnixMilli())-80*24*3600*1000)
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
