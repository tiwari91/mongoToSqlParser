package sqlconverter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type OplogInsert struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
}

func ConvertToSQLInsert(oplogJSON string) (string, string, string, error) {
	var oplog OplogInsert
	err := json.Unmarshal([]byte(oplogJSON), &oplog)
	if err != nil {
		return "", "", "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(oplog.O, &data)
	if err != nil {
		return "", "", "", err
	}

	var jsonCols []string
	var jsonValues []string
	var columnDefinitions []string
	var columnNames []string

	for key, value := range data {
		jsonCols = append(jsonCols, key)
		columnNames = append(columnNames, key)
		switch v := value.(type) {
		case string:
			jsonValues = append(jsonValues, fmt.Sprintf("'%s'", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s VARCHAR(255)", key))
		case float64:
			jsonValues = append(jsonValues, fmt.Sprintf("%v", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s FLOAT", key))
		case bool:
			jsonValues = append(jsonValues, fmt.Sprintf("%t", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s BOOLEAN", key))
		default:
			return "", "", "", fmt.Errorf("unsupported data type for column %s", key)
		}
	}

	columnsStr := strings.Join(jsonCols, ", ")
	valuesStr := strings.Join(jsonValues, ", ")

	columnDefsStr := strings.Join(columnDefinitions, ", ")

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(oplog.Ns, ".")[0])
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Ns, columnDefsStr)

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Ns, columnsStr, valuesStr)

	return createSchemaSQL, createTableSQL, insertSQL, nil
}
