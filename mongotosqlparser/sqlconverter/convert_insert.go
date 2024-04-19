package sqlconverter

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func ConvertToSQLInsert(namespace string, data map[string]interface{}) (string, error) {

	// Map to store generated schema and table creation SQL statements
	createdTables := make(map[string][]string)

	var sqlStatements []string

	var jsonValues []string
	var columnDefinitions []string
	var columnNames []string

	// Generate schema and table creation SQL statements if not already created
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(namespace, ".")[0])
	sqlStatements = append(sqlStatements, createSchemaSQL)

	for key, value := range data {
		columnNames = append(columnNames, key)
		//fmt.Println("columnNames:", columnNames)
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
		case []interface{}:
			//fmt.Println("v interface", v)
			// Handle arrays
			for _, item := range v {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return "", fmt.Errorf("unable to parse array item for column %s", key)
				}
				//fmt.Println("itemMap:", itemMap)

				// Create a table for the array if not already created
				createTable(namespace, key, itemMap, &createdTables, &sqlStatements)

				studentID := getStudentId(data)
				if studentID == "" {
					return "", fmt.Errorf("student ID not found in oplog data")
				}

				// Insert records into the array table
				insertRecords(namespace, key, itemMap, studentID, &sqlStatements)
			}
		case map[string]interface{}:
			// Handle nested objects
			createTable(namespace, key, v, &createdTables, &sqlStatements)

			studentID := getStudentId(data)
			if studentID == "" {
				return "", fmt.Errorf("student ID not found in oplog data")
			}
			insertRecords(namespace, key, v, studentID, &sqlStatements)

		default:
			return "", fmt.Errorf("unsupported data type for column %s", key)
		}
	}

	valuesStr := strings.Join(jsonValues, ", ")
	columnDefsStr := strings.Join(columnDefinitions, ", ")
	columnNamesStr := strings.Join(columnNames, ", ")

	if _, ok := createdTables[namespace]; !ok {
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", namespace, columnDefsStr)
		sqlStatements = append(sqlStatements, createTableSQL)
		createdTables[namespace] = columnNames
	}

	var alterTableSQL []string

	// Extract column names from the createdTables map
	columnNamesFromCreateTable, ok := createdTables[namespace]
	if !ok {
		return "", fmt.Errorf("no table created for namespace %s", namespace)
	}

	//fmt.Println("\ncreatedTables:", createdTables)

	for key := range data {
		if !contains(columnNamesFromCreateTable, key) {
			alterTableSQL = append(alterTableSQL, fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(255);", namespace, key))
			createdTables[namespace] = append(createdTables[namespace], key)
		}
	}

	sqlStatements = append(sqlStatements, alterTableSQL...)

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", namespace, columnNamesStr, valuesStr)
	sqlStatements = append(sqlStatements, insertSQL)

	return strings.Join(sqlStatements, "\n"), nil
}

// Function to create a table for array (nested objects)
func createTable(namespace, columnName string, data map[string]interface{}, createdTables *map[string][]string, sqlStatements *[]string) {
	tableName := fmt.Sprintf("%s.%s_%s", strings.Split(namespace, ".")[0], strings.Split(namespace, ".")[1], columnName)
	if _, ok := (*createdTables)[tableName]; !ok {
		var columnDefs []string
		columnDefs = append(columnDefs, "_id VARCHAR(255) PRIMARY KEY")

		columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", strings.Split(namespace, ".")[1]+"__id"))
		for key := range data {
			columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", key))
		}
		createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columnDefs, ", "))
		*sqlStatements = append(*sqlStatements, createTableSQL)
		(*createdTables)[tableName] = []string{"__id", strings.Split(namespace, ".")[1] + "__id"}
	}
}

// // Function to insert records into array tables
func insertRecords(namespace, columnName string, data map[string]interface{}, studentID string, sqlStatements *[]string) {
	tableName := fmt.Sprintf("%s_%s", namespace, columnName)
	var columnNames []string
	var values []string
	for key, value := range data {
		columnNames = append(columnNames, key)
		values = append(values, fmt.Sprintf("'%v'", value))
	}

	columnNames = append(columnNames, "student__id")
	values = append(values, fmt.Sprintf("'%s'", studentID))

	insertSQL := fmt.Sprintf("INSERT INTO %s (_id, %s) VALUES ('%s', %s);", tableName, strings.Join(columnNames, ", "), generateUUID(), strings.Join(values, ", "))
	*sqlStatements = append(*sqlStatements, insertSQL)
}

// // Function to generate a UUID (randomly generated _id)
func generateUUID() string {
	u := uuid.New()
	return u.String()
}
