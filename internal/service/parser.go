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
		done              = make(chan struct{})
		statementChannel  = make(chan string, 100)
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

	go func() {
		for statement := range statementChannel {
			writer.WriterStreamFile(outputFile, statement)
		}
		close(done)
	}()

	for decoder.More() {
		if err := decoder.Decode(&oplogs); err != nil {
			fmt.Printf("Error decoding JSON: %s\n", err)
			break
		}

		for _, oplog := range oplogs {
			wg.Add(1)
			go func(oplog Oplog) {
				defer wg.Done()

				var data map[string]interface{}

				processedOplogsMu.Lock()
				defer processedOplogsMu.Unlock()

				err = json.Unmarshal(oplog.O, &data)
				if err != nil {
					fmt.Printf("Error unmarshaling JSON: %s", err)
					return
				}

				switch oplog.Op {
				case "i":
					err = processInsertOperation(oplog.Ns, data, existingSchemas, createdTables, statementChannel)
				case "u":
					err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data, statementChannel)
				case "d":
					err = processDeleteOperation(oplog.Ns, data, statementChannel)
				default:
					return
				}
				if err != nil {
					fmt.Printf("Error processing operation: %s", err)
					return
				}
			}(oplog)
		}
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(statementChannel) // Close the statement channel when all tasks are done
	}()

	// Wait for the statement channel to be closed
	<-done
	return nil
}
