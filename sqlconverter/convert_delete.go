package sqlconverter

import (
	"encoding/json"
	"fmt"
)

type OplogDelete struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
}

func ConvertToSQLDelete(oplogJSON string) (string, error) {
	var oplog OplogDelete
	err := json.Unmarshal([]byte(oplogJSON), &oplog)
	if err != nil {
		return "", err
	}

	var data map[string]interface{}
	err = json.Unmarshal(oplog.O, &data)
	if err != nil {
		return "", err
	}

	var condition string
	for key, value := range data {
		if key == "_id" {
			condition = fmt.Sprintf("%s = '%v'", key, value)
			break
		}
	}

	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE %s;", oplog.Ns, condition)

	return sqlStatement, nil
}
