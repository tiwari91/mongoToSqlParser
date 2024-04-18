package sqlconverter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type OplogDelete struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
}

func ConvertToSQLDelete(oplogJSON []byte) (string, error) {
	var oplogs []OplogDelete
	err := json.Unmarshal([]byte(oplogJSON), &oplogs)
	if err != nil {
		return "", err
	}

	var sqlStatements []string

	for _, oplog := range oplogs {
		if oplog.Op == "i" || oplog.Op == "u" {
			continue
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
		sqlStatements = append(sqlStatements, sqlStatement)
	}
	return strings.Join(sqlStatements, "\n"), nil
}
