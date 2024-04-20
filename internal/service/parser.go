package service

import (
	"encoding/json"
	"sync"

	"github.com/tiwari91/mongoparser/internal/writer"
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

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go worker(&wg, oplogs, resultChannel, existingSchemas, createdTables, processedOplogs, &processedOplogsMu)
	}

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	writer.WriterFile(outputFilename, resultChannel)

	return nil
}
