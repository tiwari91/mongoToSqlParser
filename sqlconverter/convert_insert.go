package sqlconverter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type OplogInsert struct {
	Op string          `json:"op"`
	Ns string          `json:"ns"`
	O  json.RawMessage `json:"o"`
}

func ConvertToSQLInsert(oplogJSON string) (string, error) {
	var oplogs []OplogInsert
	err := json.Unmarshal([]byte(oplogJSON), &oplogs)
	if err != nil {
		return "", err
	}

	// Map to store generated schema and table creation SQL statements
	createdTables := make(map[string][]string)

	var sqlStatements []string

	for _, oplog := range oplogs {
		var data map[string]interface{}
		err = json.Unmarshal(oplog.O, &data)
		if err != nil {
			return "", err
		}

		var jsonValues []string
		var columnDefinitions []string
		var columnNames []string
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
				// Handle arrays
				for _, item := range v {
					itemMap, ok := item.(map[string]interface{})
					if !ok {
						return "", fmt.Errorf("unable to parse array item for column %s", key)
					}
					//fmt.Println("itemMap:", itemMap)

					// Create a table for the array if not already created
					createArrayTable(oplog.Ns, key, itemMap, &createdTables, &sqlStatements)

					studentID := getStudentId(data)
					// Check if student ID is empty
					if studentID == "" {
						return "", fmt.Errorf("student ID not found in oplog data")
					}

					// Insert records into the array table
					insertArrayRecord(oplog.Ns, key, itemMap, studentID, &sqlStatements)
				}
			case map[string]interface{}:
				continue
				// Handle nested objects
				// createPhoneTable(oplog.Ns, key, v, &createdTables, &sqlStatements)
				// insertPhoneRecords(oplog.Ns, key, v, studentID, &sqlStatements)

			default:
				return "", fmt.Errorf("unsupported data type for column %s", key)
			}
		}

		valuesStr := strings.Join(jsonValues, ", ")
		columnDefsStr := strings.Join(columnDefinitions, ", ")
		columnNamesStr := strings.Join(columnNames, ", ")

		// Generate schema and table creation SQL statements if not already created
		createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(oplog.Ns, ".")[0])
		if _, ok := createdTables[oplog.Ns]; !ok {
			createTableSQL := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.Ns, columnDefsStr)
			sqlStatements = append(sqlStatements, createSchemaSQL, createTableSQL)
			createdTables[oplog.Ns] = columnNames
		}

		var alterTableSQL []string

		// Extract column names from the createdTables map
		columnNamesFromCreateTable, ok := createdTables[oplog.Ns]
		if !ok {
			return "", fmt.Errorf("no table created for namespace %s", oplog.Ns)
		}

		//fmt.Println("\ncreatedTables:", createdTables)

		for key := range data {
			if !contains(columnNamesFromCreateTable, key) {
				alterTableSQL = append(alterTableSQL, fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(255);", oplog.Ns, key))
				createdTables[oplog.Ns] = append(createdTables[oplog.Ns], key)
			}
		}

		sqlStatements = append(sqlStatements, alterTableSQL...)

		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.Ns, columnNamesStr, valuesStr)
		sqlStatements = append(sqlStatements, insertSQL)
	}

	return strings.Join(sqlStatements, "\n"), nil
}

// Function to create a table for array (nested objects)
func createArrayTable(namespace, columnName string, data map[string]interface{}, createdTables *map[string][]string, sqlStatements *[]string) {
	tableName := fmt.Sprintf("%s.%s_%s", strings.Split(namespace, ".")[0], strings.Split(namespace, ".")[1], columnName)
	if _, ok := (*createdTables)[tableName]; !ok {
		var columnDefs []string
		columnDefs = append(columnDefs, "_id VARCHAR(255) PRIMARY KEY")

		columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", strings.Split(namespace, ".")[1]+"__id"))
		for key := range data {
			columnDefs = append(columnDefs, fmt.Sprintf("%s VARCHAR(255)", key))
		}
		createTableSQL := fmt.Sprintf("CREATE TABLE %s (%s);", tableName, strings.Join(columnDefs, ", "))
		*sqlStatements = append(*sqlStatements, createTableSQL)
		(*createdTables)[tableName] = []string{"__id", strings.Split(namespace, ".")[1] + "__id"}
	}
}

// // Function to insert records into array tables
func insertArrayRecord(namespace, columnName string, data map[string]interface{}, studentID string, sqlStatements *[]string) {
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
