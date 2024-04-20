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
