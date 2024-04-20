package domain

import (
	"fmt"
	"strings"

	"github.com/tiwari91/mongoparser/internal/utils"
)

func InsertRecords(namespace, columnName string, data map[string]interface{}, studentID string, output chan<- string, createdTables map[string][]string) {
	tableName := fmt.Sprintf("%s_%s", namespace, columnName)

	if columns, ok := createdTables[tableName]; ok {
		var columnNames []string
		var values []string

		for _, column := range columns {
			if value, ok := data[column]; ok {
				columnNames = append(columnNames, column)
				values = append(values, fmt.Sprintf("'%v'", value))
			}
		}

		columnNames = append(columnNames, "student__id")
		values = append(values, fmt.Sprintf("'%s'", studentID))

		insertSQL := fmt.Sprintf("INSERT INTO %s (_id, %s) VALUES ('%s', %s);", tableName, strings.Join(columnNames, ", "), utils.GenerateUUID(), strings.Join(values, ", "))
		output <- insertSQL
	}
}
