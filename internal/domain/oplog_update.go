package domain

import (
	"fmt"
	"strings"
)

func ConvertToSQLUpdate(namespace string, ID string, data map[string]interface{}, resultChannel chan<- string) error {
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

	condition := fmt.Sprintf("_id = '%s'", ID)
	updateStr := strings.Join(updateFields, ", ")
	sqlStatement := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", namespace, updateStr, condition)

	resultChannel <- sqlStatement

	return nil
}
