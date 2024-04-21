package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

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

	existingSchemas := make(map[string]bool)
	createdTables := make(map[string][]string)

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

	var statement string

	for decoder.More() {

		if err := decoder.Decode(&oplogs); err != nil {
			fmt.Printf("Error decoding JSON: %s\n", err)
			break
		}

		for _, oplog := range oplogs {

			var data map[string]interface{}
			err = json.Unmarshal(oplog.O, &data)
			if err != nil {
				fmt.Printf("Error unmarshaling JSON: %s", err)
				continue
			}

			switch oplog.Op {
			case "i":
				statement, err = processInsertOperation(oplog.Ns, data, existingSchemas, createdTables)
				if err != nil {
					fmt.Printf("Error processing insert JSON: %s", err)
					continue
				}
			case "u":
				statement, err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data)
				if err != nil {
					fmt.Printf("Error processing update JSON: %s", err)
					continue
				}
			case "d":
				statement, err = processDeleteOperation(oplog.Ns, data)
				if err != nil {
					fmt.Printf("Error processing delete JSON: %s", err)
					continue
				}
			default:
				continue
			}

			writer.WriterStreamFile(outputFile, statement)
		}

	}

	return nil
}
