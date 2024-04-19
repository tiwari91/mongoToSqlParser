package sqlconverter

import (
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

func ProcessLogFile(oplogJSON, outputFilename string) error {
	var oplogs []Oplog
	err := json.Unmarshal([]byte(oplogJSON), &oplogs)
	if err != nil {
		return err
	}

	// Initialize existingSchemas map
	existingSchemas := make(map[string]bool)
	createdTables := make(map[string][]string)

	var sqlStatements []string

	//fmt.Println("oplogs", oplogs)
	for _, oplog := range oplogs {
		var data map[string]interface{}
		err = json.Unmarshal(oplog.O, &data)
		if err != nil {
			return err
		}

		switch oplog.Op {
		case "i":
			//	fmt.Println("data", data)
			sqlStatement, err := ConvertToSQLInsert(oplog.Ns, data, existingSchemas, createdTables)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			sqlStatements = append(sqlStatements, sqlStatement)
		case "u":
			sqlStatement, err := ConvertToSQLUpdate(oplog.Ns, oplog.O2.ID, data)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			sqlStatements = append(sqlStatements, sqlStatement)
			//continue
		case "d":
			sqlStatement, err := ConvertToSQLDelete(oplog.Ns, data)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			sqlStatements = append(sqlStatements, sqlStatement)
			//continue
		default:
			continue
		}
	}

	// Open the file in append mode and write the unique SQL statements
	file, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}
	defer file.Close()

	for _, sqlStatement := range sqlStatements {
		_, err := file.WriteString(sqlStatement + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}
