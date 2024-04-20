package sqlconverter

import (
	"fmt"
	"strings"
)

func ConvertToSQLInsert(namespace string, data map[string]interface{},
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
		if isNested(value) {
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
	if tableExists(namespace, createdTables) {
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

				studentID := getStudentId(data)
				if studentID == "" {
					return fmt.Errorf("student ID not found in oplog data")
				}

				// Insert records into the array table
				insertRecords(namespace, key, itemMap, studentID, output, createdTables)
			}
		case map[string]interface{}:
			// Handle nested objects
			createTable(namespace, key, v, &createdTables, output)

			studentID := getStudentId(data)
			if studentID == "" {
				return fmt.Errorf("student ID not found in oplog data")
			}
			insertRecords(namespace, key, v, studentID, output, createdTables)
		default:
			return fmt.Errorf("unsupported data type for nested column %s", key)
		}
	}

	return nil
}

// Function to alter the table if needed
func alterTable(columnNames []string, createdTables map[string][]string, namespace string, output chan<- string) {
	for _, columnName := range columnNames {
		if !contains(createdTables[namespace], columnName) {
			alterTableSQL := fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(255);", namespace, columnName)
			createdTables[namespace] = append(createdTables[namespace], columnName)
			output <- alterTableSQL
		}
	}
}

// Function to check if a value is nested (array or object)
func isNested(value interface{}) bool {
	switch value.(type) {
	case []interface{}, map[string]interface{}:
		return true
	default:
		return false
	}
}

// Function to create a table for array (nested objects)
func createTable(namespace, columnName string, data map[string]interface{}, createdTables *map[string][]string, output chan<- string) {
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
			if !contains((*createdTables)[tableName], key) {
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

// Function to insert records into array tables
func insertRecords(namespace, columnName string, data map[string]interface{}, studentID string, output chan<- string, createdTables map[string][]string) {
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

		insertSQL := fmt.Sprintf("INSERT INTO %s (_id, %s) VALUES ('%s', %s);", tableName, strings.Join(columnNames, ", "), generateUUID(), strings.Join(values, ", "))
		output <- insertSQL
	}
}

// Function to check if a table exists in the database schema
func tableExists(namespace string, existingSchemas map[string][]string) bool {
	_, exists := existingSchemas[namespace]
	return exists
}
