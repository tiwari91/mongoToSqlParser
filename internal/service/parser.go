package service

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
)

type Oplog struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
	O2 struct {
		ID string `json:"_id"`
	} `json:"o2"`
}

func ProcessLogFile(db *sql.DB, filename string) error {
	var oplogs []Oplog

	// Initialize existingSchemas map
	existingSchemas := make(map[string]bool)
	createdTables := make(map[string][]string)

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	outputFile, err := os.Create("db/result.sql")
	if err != nil {
		return err
	}
	defer outputFile.Close()

	// Create a buffered writer to efficiently write to the output file
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	// Create JSON decoder
	decoder := json.NewDecoder(bufio.NewReader(file))

	var statement string

	// Read and process each JSON object in the file
	for decoder.More() {

		if err := decoder.Decode(&oplogs); err != nil {
			// Handle JSON decoding error
			fmt.Printf("Error decoding JSON: %s\n", err)
			break
		}

		// Print the content of each oplog
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
			case "u":
				statement, err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data)
			case "d":
				statement, err = processDeleteOperation(oplog.Ns, data)
			default:
				continue
			}

			// Write the statement to the output file
			_, err := writer.WriteString(statement + "\n")
			if err != nil {
				return err
			}
		}

	}

	return nil
}
