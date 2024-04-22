package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tiwari91/mongoparser/internal/domain"
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

	//outputFile, err := os.Create(outputFilename)
	outputFile, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

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

		for index, oplog := range oplogs {

			var data map[string]interface{}
			err = json.Unmarshal(oplog.O, &data)
			if err != nil {
				fmt.Println("Error unmarshaling JSON")
				continue
			}

			exists, err := domain.PositionExists(db, index)
			if err != nil {
				fmt.Println("Error checking position existence")
				continue
			}
			if exists {
				continue
			}

			err = domain.SavePosition(db, index)
			if err != nil {
				fmt.Println("Error saving position")
				continue
			}

			switch oplog.Op {
			case "i":
				statement, err = processInsertOperation(oplog.Ns, data, existingSchemas, createdTables)
				if err != nil {
					fmt.Println("Error processing insert JSON")
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
