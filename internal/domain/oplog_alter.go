package domain

import (
	"fmt"

	"github.com/tiwari91/mongoparser/internal/utils"
)

func alterTable(columnNames []string, createdTables map[string][]string, namespace string, output chan<- string) {
	for _, columnName := range columnNames {
		if !utils.Contains(createdTables[namespace], columnName) {
			alterTableSQL := fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(255);", namespace, columnName)
			createdTables[namespace] = append(createdTables[namespace], columnName)
			output <- alterTableSQL
		}
	}
}
