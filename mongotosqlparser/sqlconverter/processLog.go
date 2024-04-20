package sqlconverter

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
)

type Oplog struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
	O2 struct {
		ID string `json:"_id"`
	} `json:"o2"`
}

func ProcessLogFile(oplogJSON, outputFilename string) error {
	var oplogs []Oplog
	err := json.Unmarshal([]byte(oplogJSON), &oplogs)
	if err != nil {
		return err
	}

	// Initialize existingSchemas map
	existingSchemas := make(map[string]bool)
	createdTables := make(map[string][]string)

	var wg sync.WaitGroup
	resultChannel := make(chan string, len(oplogs))

	// Start worker pool
	//fmt.Println("len(oplogs)", len(oplogs))

	processedOplogs := make(map[string]bool)
	var processedOplogsMu sync.Mutex

	for i := 0; i < len(oplogs); i++ {
		wg.Add(1)
		go worker(&wg, oplogs, resultChannel, existingSchemas,
			createdTables, processedOplogs, &processedOplogsMu)
	}

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	// Collect results
	file, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for result := range resultChannel {
		_, err := file.WriteString(result + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func worker(wg *sync.WaitGroup, oplogs []Oplog, resultChannel chan<- string,
	existingSchemas map[string]bool, createdTables map[string][]string,
	processedOplogs map[string]bool, processedOplogsMu *sync.Mutex) {

	defer wg.Done()

	processedOplogsMu.Lock()
	defer processedOplogsMu.Unlock()

	for index, oplog := range oplogs {
		if processedOplogs[strconv.Itoa(index)] {
			continue
		}
		processedOplogs[strconv.Itoa(index)] = true

		var data map[string]interface{}
		err := json.Unmarshal(oplog.O, &data)
		if err != nil {
			resultChannel <- fmt.Sprintf("Error unmarshaling JSON: %s", err)
			continue
		}

		switch oplog.Op {
		case "i":
			err = ConvertToSQLInsert(oplog.Ns, data, existingSchemas, createdTables, resultChannel)
		case "u":
			err = ConvertToSQLUpdate(oplog.Ns, oplog.O2.ID, data, resultChannel)
		case "d":
			err = ConvertToSQLDelete(oplog.Ns, data, resultChannel)
		default:
			continue
		}

		if err != nil {
			resultChannel <- fmt.Sprintf("Error: %s", err)
			continue
		}
	}
}
