package sqlconverter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type OplogUpdate struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
	O2 struct {
		ID string `json:"_id"`
	} `json:"o2"`
}

func ConvertToSQLUpdate(oplogJSON []byte) (string, error) {
	var oplogs []OplogUpdate

	err := json.Unmarshal(oplogJSON, &oplogs)
	if err != nil {
		return "", err
	}

	var sqlStatements []string

	for _, oplog := range oplogs {
		var data map[string]interface{}
		err = json.Unmarshal(oplog.O, &data)
		if err != nil {
			return "", err
		}

		var updateFields []string

		for _, value := range data {
			switch diff := value.(type) {
			case map[string]interface{}:
				for opType, fields := range diff {
					for field, newValue := range fields.(map[string]interface{}) {
						if opType == "d" {
							updateFields = append(updateFields, fmt.Sprintf("%s = NULL", field))
						} else {
							updateFields = append(updateFields, fmt.Sprintf("%s = %v", field, newValue))
						}
					}
				}
			}
		}

		condition := fmt.Sprintf("_id = '%s'", oplog.O2.ID)
		updateStr := strings.Join(updateFields, ", ")
		sqlStatement := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", oplog.Ns, updateStr, condition)
		sqlStatements = append(sqlStatements, sqlStatement)

	}

	return strings.Join(sqlStatements, "\n"), nil
}
