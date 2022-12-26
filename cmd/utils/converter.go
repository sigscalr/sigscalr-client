package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func convertTSVToJson(input string, output string) error {
	sTime := time.Now()
	file, err := os.Open(input)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return err
	}
	columns := strings.Split(scanner.Text(), "\t")
	log.Infof("Assuming columns %+v for file %+v", columns, input)

	jsonValue := make(map[string]interface{}, len(columns))
	for _, column := range columns {
		jsonValue[column] = nil
	}

	outFile, err := os.OpenFile(output, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer outFile.Close()

	count := 0
	for scanner.Scan() {
		count++

		if count%1000 == 0 {
			log.Infof("Still converting file. Total time %+v. Number of lines %+v", time.Since(sTime), count)
		}
		rawValues := strings.Split(scanner.Text(), "\t")
		for idx, value := range columns {
			if idx >= len(rawValues) {
				jsonValue[value] = nil
			} else {
				jsonValue[value] = rawValues[idx]
			}
		}
		rawVal, err := json.Marshal(jsonValue)
		if err != nil {
			log.Errorf("Failed to marshal json! %v", err)
			continue
		}
		outFile.Write(rawVal)
		outFile.WriteString("\n")
	}
	log.Infof("Finished converting file. Total time %+v. Number of lines %+v", time.Since(sTime), count)
	return scanner.Err()
}

func main() {
	inputPtr := flag.String("input", "", "input tsv file to convert")
	outputPtr := flag.String("output", "", "output of json file")

	flag.Parse()
	inputStr, outputStr := *inputPtr, *outputPtr

	if len(inputStr) == 0 {
		log.Fatal("Input file cannot be empty")
	}

	if outputStr == inputStr {
		log.Fatalf("Input string %+v cannot be the same as the output string %+v", inputStr, outputStr)
	}
	if len(outputStr) == 0 {
		outputStr = inputStr + ".json"
	}
	log.Infof("Converting %+v to a json file at %+v:", inputStr, outputStr)
	err := convertTSVToJson(inputStr, outputStr)
	if err != nil {
		log.Fatalf("Failed to convert %+v to json: %v", inputStr, err)
	}
}
