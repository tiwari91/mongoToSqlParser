package service

import (
	"fmt"
	"strings"

	"github.com/tiwari91/mongoparser/internal/domain"
	"github.com/tiwari91/mongoparser/internal/utils"
)

func ProcessInsert(namespace string, data map[string]interface{},
	existingSchemas map[string]bool, createdTables map[string][]string,
	output chan<- string) error {

	var nonNestedData = make(map[string]interface{})
	var nestedData = make(map[string]interface{})

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(namespace, ".")[0])
	if _, ok := existingSchemas[strings.Split(namespace, ".")[0]]; !ok {
		output <- createSchemaSQL
		existingSchemas[strings.Split(namespace, ".")[0]] = true
	}

	// Separate non-nested data and nested data
	for key, value := range data {
		if utils.IsNested(value) {
			nestedData[key] = value
		} else {
			nonNestedData[key] = value
		}
	}

	// Process non-nested data first
	var columnNames []string
	var jsonValues []string
	var columnDefinitions []string

	for key, value := range nonNestedData {
		columnNames = append(columnNames, key)
		switch v := value.(type) {
		case string:
			jsonValues = append(jsonValues, fmt.Sprintf("'%s'", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s VARCHAR(255)", key))
		case float64:
			jsonValues = append(jsonValues, fmt.Sprintf("%v", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s FLOAT", key))
		case bool:
			jsonValues = append(jsonValues, fmt.Sprintf("%t", v))
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s BOOLEAN", key))
		default:
			return fmt.Errorf("unsupported data type for non-nested column %s", key)
		}
	}

	// Generate SQL for non-nested data
	valuesStr := strings.Join(jsonValues, ", ")
	columnDefsStr := strings.Join(columnDefinitions, ", ")

	// Check if the table already exists
	if utils.TableExists(namespace, createdTables) {
		// If the table exists and columns are not the same then perform alterations
		domain.AlterTable(columnNames, createdTables, namespace, output)

	} else {
		// If the table does not exist, create it
		createTableSQL := fmt.Sprintf("CREATE TABLE %s IF NOT EXISTS (%s);", namespace, columnDefsStr)
		output <- createTableSQL
		createdTables[namespace] = append(createdTables[namespace], columnNames...)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", namespace, strings.Join(columnNames, ", "), valuesStr)
	output <- insertSQL

	// Process nested data
	for key, value := range nestedData {
		switch v := value.(type) {
		case []interface{}:
			// Handle arrays
			for _, item := range v {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return fmt.Errorf("unable to parse array item for column %s", key)
				}

				// Create a table for the array if not already created
				domain.CreateTable(namespace, key, itemMap, &createdTables, output)

				studentID := utils.GetStudentId(data)
				if studentID == "" {
					return fmt.Errorf("student ID not found in oplog data")
				}

				// Insert records into the array table
				domain.InsertRecords(namespace, key, itemMap, studentID, output, createdTables)
			}
		case map[string]interface{}:
			// Handle nested objects
			domain.CreateTable(namespace, key, v, &createdTables, output)

			studentID := utils.GetStudentId(data)
			if studentID == "" {
				return fmt.Errorf("student ID not found in oplog data")
			}
			domain.InsertRecords(namespace, key, v, studentID, output, createdTables)
		default:
			return fmt.Errorf("unsupported data type for nested column %s", key)
		}
	}

	return nil
}
