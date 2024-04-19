package sqlconverter

import (
	"fmt"
	"strings"
)

func ConvertToSQLDelete(namespace string, data map[string]interface{}) (string, error) {

	var sqlStatements []string
	var condition string

	for key, value := range data {
		if key == "_id" {
			condition = fmt.Sprintf("%s = '%v'", key, value)
			break
		}
	}

	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE %s;", namespace, condition)
	sqlStatements = append(sqlStatements, sqlStatement)

	return strings.Join(sqlStatements, "\n"), nil
}
