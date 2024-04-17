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
	var oplog OplogInsert
	err := json.Unmarshal([]byte(oplogJSON), &oplog)
	if err != nil {
		return "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(oplog.O, &data)
	if err != nil {
		return "", err
	}

	var jsonCols []string
	var jsonValues []string
	for key, value := range data {
		jsonCols = append(jsonCols, key)
		switch v := value.(type) {
		case string:
			jsonValues = append(jsonValues, fmt.Sprintf("'%s'", v))
		case float64:
			jsonValues = append(jsonValues, fmt.Sprintf("%v", v))
		case bool:
			jsonValues = append(jsonValues, fmt.Sprintf("%t", v))
		default:
			return "", fmt.Errorf("unsupported data type for column %s", key)
		}
	}

	columnsStr := strings.Join(jsonCols, ", ")
	valuesStr := strings.Join(jsonValues, ", ")

	sqlStatement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Ns, columnsStr, valuesStr)

	return sqlStatement, nil
}
