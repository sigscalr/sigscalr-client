package utils

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/brianvoe/gofakeit/v6"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastrand"
)

var json = jsoniter.ConfigFastest

type Generator interface {
	Init(fName ...string) error
	GetLogLine() ([]byte, error)
	GetRawLog() (map[string]interface{}, error)
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
type StaticGenerator struct {
	logLine []byte
	ts      bool
}
type K8sGenerator struct {
	baseBody  map[string]interface{}
	tNowEpoch uint64
	ts        bool
	faker     *gofakeit.Faker
	seed      int64
}

type DynamicUserGenerator struct {
	baseBody  map[string]interface{}
	tNowEpoch uint64
	ts        bool
	faker     *gofakeit.Faker
	seed      int64
}

func InitDynamicUserGenerator(ts bool, seed int64) *DynamicUserGenerator {
	return &DynamicUserGenerator{
		ts:   ts,
		seed: seed,
	}
}

func InitK8sGenerator(ts bool, seed int64) *K8sGenerator {

	return &K8sGenerator{
		ts:   ts,
		seed: seed,
	}
}

func InitStaticGenerator(ts bool) *StaticGenerator {
	return &StaticGenerator{
		ts: ts,
	}
}

func InitFileReader() *FileReader {
	return &FileReader{}
}

type msg struct {
	Msg         string
	DomainName  string
	Hostname    string
	HTTPStatus  int
	Batch       string
	Region      string
	Latency     int
	Az          string
	Url         string
	UserAgent   string
	IPv4Address string
	Port        int
}

var logMessages = []string{
	"%s for DRA plugin %q failed. Plugin returned an empty list for supported versions",
	"%s for DRA plugin %q failed. None of the versions specified %q are supported. err='%v'",
	"Unable to write event '%v' (retry limit exceeded!)",
	"Unable to start event watcher: '%v' (will not retry!)",
	"Could not construct reference to: '%v' due to: '%v'. Will not report event: '%v' '%v' '%v'",
}

func replacePlaceholders(template string) string {

	placeholderRegex := regexp.MustCompile(`(['"]%[^\s%]+['"]|(%[^\s%]))`)

	indices := placeholderRegex.FindStringIndex(template)
	for len(indices) > 0 {
		start := indices[0]
		end := indices[1]
		placeholderType := template[start:end]
		placeholderType = strings.Replace(placeholderType, "%", "", 1)
		placeholderType = strings.Replace(placeholderType, "'", "", 2)
		var replacement string
		switch placeholderType {
		case "s":
			replacement = gofakeit.Word()
			template = string(template[:start]) + "" + replacement + "" + string(template[end:])

		case "q":
			replacement = gofakeit.BuzzWord()
			template = string(template[:start]) + "" + replacement + "" + string(template[end:])

		case "v":
			replacement = fmt.Sprintf("%d", gofakeit.Number(1, 100))
			template = string(template[:start]) + "" + replacement + "" + string(template[end:])
		default:

			replacement = "UNKNOWN"
			template = string(template[:start]) + "" + replacement + "" + string(template[end:])

		}
		indices = placeholderRegex.FindStringIndex(template)
	}
	return template
}

func randomizeBody(f *gofakeit.Faker, m map[string]interface{}, addts bool) {

	m["batch"] = fmt.Sprintf("batch-%d", f.Number(1, 1000))
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
	m["weekday"] = f.WeekDay()
	m["http_method"] = f.HTTPMethod()
	m["http_status"] = f.HTTPStatusCodeSimple()
	m["app_name"] = f.AppName()
	m["app_version"] = f.AppVersion()
	m["ident"] = f.UUID()
	m["user_agent"] = f.UserAgent()
	m["url"] = f.URL()
	m["group"] = fmt.Sprintf("group %d", f.Number(0, 2))
	m["question"] = f.Question()
	m["latency"] = f.Number(0, 10_000_000)

	if addts {
		m["timestamp"] = uint64(time.Now().UnixMilli())
	}
}

func (r *DynamicUserGenerator) generateRandomBody() {
	randomizeBody(r.faker, r.baseBody, r.ts)
}

func (r *K8sGenerator) createK8sBody() {
	randomTemplate := logMessages[gofakeit.Number(0, len(logMessages)-1)]
	logEntry := msg{}
	logEntry.Batch = fmt.Sprintf("batch-%d", r.faker.Number(1, 1000))
	logEntry.DomainName = r.faker.DomainName()
	logEntry.Hostname = r.faker.IPv4Address()
	logEntry.HTTPStatus = r.faker.HTTPStatusCodeSimple()
	logEntry.Latency = r.faker.Number(0, 100)
	logEntry.Region = r.faker.TimeZoneRegion()
	logEntry.Az = r.faker.Country()
	logEntry.UserAgent = r.faker.UserAgent()
	logEntry.Url = r.faker.URL()
	logEntry.IPv4Address = r.faker.IPv4Address()
	logEntry.Port = r.faker.Number(0, 65535)
	logEntry.Msg = replacePlaceholders(randomTemplate)

	r.baseBody["batch"] = logEntry.Batch
	r.baseBody["DomainName"] = logEntry.DomainName
	r.baseBody["Region"] = logEntry.Region
	r.baseBody["Az"] = logEntry.Az
	r.baseBody["hostname"] = logEntry.Hostname
	r.baseBody["httpStatus"] = logEntry.HTTPStatus
	r.baseBody["UserAgent"] = logEntry.UserAgent
	r.baseBody["Url"] = logEntry.Url
	r.baseBody["latency"] = logEntry.Latency
	r.baseBody["IPv4Address"] = logEntry.IPv4Address
	r.baseBody["Port"] = logEntry.Port

	r.baseBody["msg"] = logEntry.Msg

}

func (r *K8sGenerator) Init(fName ...string) error {
	gofakeit.Seed(r.seed)
	r.faker = gofakeit.NewUnlocked(r.seed)
	rand.Seed(r.seed)
	r.baseBody = make(map[string]interface{})
	r.createK8sBody()
	body, err := json.Marshal(r.baseBody)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	r.tNowEpoch = uint64(time.Now().UnixMilli()) - 80*24*3600*1000
	return nil
}

func (r *DynamicUserGenerator) Init(fName ...string) error {
	gofakeit.Seed(r.seed)
	r.faker = gofakeit.NewUnlocked(r.seed)
	rand.Seed(r.seed)
	r.baseBody = make(map[string]interface{})
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

func (r *K8sGenerator) GetLogLine() ([]byte, error) {
	r.createK8sBody()
	return json.Marshal(r.baseBody)
}

func (r *DynamicUserGenerator) GetLogLine() ([]byte, error) {
	r.generateRandomBody()
	return json.Marshal(r.baseBody)
}

func (r *DynamicUserGenerator) GetRawLog() (map[string]interface{}, error) {
	r.generateRandomBody()
	return r.baseBody, nil
}

func (r *K8sGenerator) GetRawLog() (map[string]interface{}, error) {
	r.createK8sBody()
	return r.baseBody, nil
}

func (r *StaticGenerator) Init(fName ...string) error {
	m := make(map[string]interface{})
	f := gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000)))
	randomizeBody(f, m, r.ts)
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of event log line is %+v bytes", stringSize)
	r.logLine = body
	return nil
}
func (sr *StaticGenerator) GetLogLine() ([]byte, error) {
	return sr.logLine, nil
}

func (sr *StaticGenerator) GetRawLog() (map[string]interface{}, error) {
	final := make(map[string]interface{})
	err := json.Unmarshal(sr.logLine, &final)
	if err != nil {
		return nil, err
	}
	return final, nil
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

func (fr *FileReader) GetRawLog() (map[string]interface{}, error) {
	rawLog, err := fr.GetLogLine()
	if err != nil {
		return nil, err
	}
	final := make(map[string]interface{})
	err = json.Unmarshal(rawLog, &final)
	if err != nil {
		return nil, err
	}
	return final, nil
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
