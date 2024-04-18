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

func ConvertToSQLInsert(oplogJSON string) (string, error) {
	var oplogs []OplogInsert
	err := json.Unmarshal([]byte(oplogJSON), &oplogs)
	if err != nil {
		return "", err
	}

	// Map to store generated schema and table creation SQL statements
	createdTables := make(map[string]bool)

	var sqlStatements []string

	for _, oplog := range oplogs {
		var data map[string]interface{}
		err = json.Unmarshal(oplog.O, &data)
		if err != nil {
			return "", err
		}

		var jsonValues []string
		var columnDefinitions []string
		var columnNames []string
		for key, value := range data {
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
				return "", fmt.Errorf("unsupported data type for column %s", key)
			}
		}

		valuesStr := strings.Join(jsonValues, ", ")
		columnDefsStr := strings.Join(columnDefinitions, ", ")
		columnNamesStr := strings.Join(columnNames, ", ")

		createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(oplog.Ns, ".")[0])
		if _, ok := createdTables[oplog.Ns]; !ok {
			createTableSQL := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Ns, columnDefsStr)
			sqlStatements = append(sqlStatements, createSchemaSQL, createTableSQL)
			createdTables[oplog.Ns] = true
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Ns, columnNamesStr, valuesStr)
		sqlStatements = append(sqlStatements, insertSQL)
	}

	return strings.Join(sqlStatements, "\n"), nil
}
