package domain

import (
	"fmt"
	"strings"

	"github.com/tiwari91/mongoparser/internal/utils"
)

func CreateTable(namespace, columnName string, data map[string]interface{}, createdTables *map[string][]string, output chan<- string) {
	tableName := fmt.Sprintf("%s.%s_%s", strings.Split(namespace, ".")[0], strings.Split(namespace, ".")[1], columnName)
	if len((*createdTables)[tableName]) == 0 {
		var columnDefs []string
		columnDefs = append(columnDefs, "_id VARCHAR(255) PRIMARY KEY")
		columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", strings.Split(namespace, ".")[1]+"__id"))
		for key := range data {
			columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", key))
		}
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columnDefs, ", "))
		output <- createTableSQL
		(*createdTables)[tableName] = []string{"__id", strings.Split(namespace, ".")[1] + "__id"}

		for key := range data {
			(*createdTables)[tableName] = append((*createdTables)[tableName], key)
		}

	} else {
		// If the table already exists, add the new columns to it
		var alterColumns []string

		for key := range data {
			if !utils.Contains((*createdTables)[tableName], key) {
				alterColumns = append(alterColumns, fmt.Sprintf("ADD %s VARCHAR(255)", key))
				(*createdTables)[tableName] = append((*createdTables)[tableName], key)
			}
		}
		if len(alterColumns) > 0 {
			alterTableSQL := fmt.Sprintf("ALTER TABLE %s %s;", tableName, strings.Join(alterColumns, ", "))
			output <- alterTableSQL
		}
	}
}

func InsertTable(namespace, columnName string, data map[string]interface{}, studentID string, output chan<- string, createdTables map[string][]string) {
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

func AlterTable(columnNames []string, createdTables map[string][]string, namespace string, output chan<- string) {
	for _, columnName := range columnNames {
		if !utils.Contains(createdTables[namespace], columnName) {
			alterTableSQL := fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(255);", namespace, columnName)
			createdTables[namespace] = append(createdTables[namespace], columnName)
			output <- alterTableSQL
		}
	}
}
