package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
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

func ProcessLogFile(db *sql.DB, inputFilename, outputFilename string) error {
	var oplogs []Oplog

	var (
		processedOplogsMu sync.Mutex
		wg                sync.WaitGroup
		existingSchemas   = make(map[string]bool)
		createdTables     = make(map[string][]string)
	)

	inputFile, err := os.Open(inputFilename)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	decoder := json.NewDecoder(bufio.NewReader(inputFile))

	for decoder.More() {
		if err := decoder.Decode(&oplogs); err != nil {
			fmt.Printf("Error decoding JSON: %s\n", err)
			break
		}

		for _, oplog := range oplogs {
			wg.Add(1)
			go func(oplog Oplog) {
				defer wg.Done()

				processedOplogsMu.Lock()
				defer processedOplogsMu.Unlock()

				var data map[string]interface{}
				err = json.Unmarshal(oplog.O, &data)
				if err != nil {
					fmt.Printf("Error unmarshaling JSON: %s", err)
					return
				}

				var statement string
				switch oplog.Op {
				case "i":
					statement, err = processInsertOperation(oplog.Ns, data, existingSchemas, createdTables)
				case "u":
					statement, err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data)
				case "d":
					statement, err = processDeleteOperation(oplog.Ns, data)
				default:
					return
				}
				if err != nil {
					fmt.Printf("Error processing operation: %s", err)
					return
				}
				writer.WriterStreamFile(outputFile, statement)
			}(oplog)
		}
	}

	wg.Wait()
	return nil
}
