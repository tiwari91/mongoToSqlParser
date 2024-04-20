package service

import (
	"fmt"
)

func ConvertToSQLDelete(namespace string, data map[string]interface{},
	resultChannel chan<- string) error {

	var condition string

	for key, value := range data {
		if key == "_id" {
			condition = fmt.Sprintf("%s = '%v'", key, value)
			break
		}
	}

	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE %s;", namespace, condition)
	resultChannel <- sqlStatement

	return nil
}
