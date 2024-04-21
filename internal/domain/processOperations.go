package domain

import (
	"fmt"
	"strings"

	"github.com/tiwari91/mongoparser/internal/utils"
)

func ProcessInsertOpertion(namespace string, data map[string]interface{}, existingSchemas map[string]bool,
	createdTables map[string][]string, output chan<- string) error {

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
		alterTable(columnNames, createdTables, namespace, output)

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
				createTable(namespace, key, itemMap, &createdTables, output)

				studentID := utils.GetStudentId(data)
				if studentID == "" {
					return fmt.Errorf("student ID not found in oplog data")
				}

				// Insert records into the array table
				insertTable(namespace, key, itemMap, studentID, output, createdTables)
			}
		case map[string]interface{}:
			// Handle nested objects
			createTable(namespace, key, v, &createdTables, output)

			studentID := utils.GetStudentId(data)
			if studentID == "" {
				return fmt.Errorf("student ID not found in oplog data")
			}
			insertTable(namespace, key, v, studentID, output, createdTables)
		default:
			return fmt.Errorf("unsupported data type for nested column %s", key)
		}
	}

	return nil
}

func ProcessUpdateOperation(namespace string, ID string, data map[string]interface{}, resultChannel chan<- string) error {
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

func ProcessDeleteOperation(namespace string, data map[string]interface{}, resultChannel chan<- string) error {

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
