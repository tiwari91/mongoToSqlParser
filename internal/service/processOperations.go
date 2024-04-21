package service

import (
	"fmt"
	"strings"

	"github.com/tiwari91/mongoparser/internal/domain"
	"github.com/tiwari91/mongoparser/internal/utils"
)

func processInsertOperation(namespace string, data map[string]interface{}, existingSchemas map[string]bool,
	createdTables map[string][]string) (string, error) {

	var sqlStatements []string

	var nonNestedData = make(map[string]interface{})
	var nestedData = make(map[string]interface{})

	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", strings.Split(namespace, ".")[0])
	if _, ok := existingSchemas[strings.Split(namespace, ".")[0]]; !ok {
		//output <- createSchemaSQL
		sqlStatements = append(sqlStatements, createSchemaSQL)
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
			return "", fmt.Errorf("unsupported data type for non-nested column %s", key)
		}
	}

	// Generate SQL for non-nested data
	valuesStr := strings.Join(jsonValues, ", ")
	columnDefsStr := strings.Join(columnDefinitions, ", ")

	// Check if the table already exists
	if utils.TableExists(namespace, createdTables) {
		// If the table exists and columns are not the same then perform alterations
		domain.AlterTable(columnNames, createdTables, namespace, &sqlStatements)

	} else {
		// If the table does not exist, create it
		createTableSQL := fmt.Sprintf("CREATE TABLE %s IF NOT EXISTS (%s);", namespace, columnDefsStr)
		sqlStatements = append(sqlStatements, createTableSQL)
		createdTables[namespace] = append(createdTables[namespace], columnNames...)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", namespace, strings.Join(columnNames, ", "), valuesStr)
	sqlStatements = append(sqlStatements, insertSQL)

	// Process nested data
	for key, value := range nestedData {
		switch v := value.(type) {
		case []interface{}:
			// Handle arrays
			for _, item := range v {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					return "", fmt.Errorf("unable to parse array item for column %s", key)
				}

				// Create a table for the array if not already created
				domain.CreateTable(namespace, key, itemMap, &createdTables, &sqlStatements)

				studentID := utils.GetStudentId(data)
				if studentID == "" {
					return "", fmt.Errorf("student ID not found in oplog data")
				}

				// Insert records into the array table
				domain.InsertTable(namespace, key, itemMap, studentID, createdTables, &sqlStatements)
			}
		case map[string]interface{}:
			// Handle nested objects
			domain.CreateTable(namespace, key, v, &createdTables, &sqlStatements)

			studentID := utils.GetStudentId(data)
			if studentID == "" {
				return "", fmt.Errorf("student ID not found in oplog data")
			}
			domain.InsertTable(namespace, key, v, studentID, createdTables, &sqlStatements)
		default:
			return "", fmt.Errorf("unsupported data type for nested column %s", key)
		}
	}

	return strings.Join(sqlStatements, "\n"), nil
}

func processUpdateOperation(namespace string, ID string, data map[string]interface{}) (string, error) {
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

	//resultChannel <- sqlStatement
	//result = append(result, sqlStatement)

	return sqlStatement, nil
}

func processDeleteOperation(namespace string, data map[string]interface{}) (string, error) {

	var condition string

	for key, value := range data {
		if key == "_id" {
			condition = fmt.Sprintf("%s = '%v'", key, value)
			break
		}
	}

	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE %s;", namespace, condition)

	return sqlStatement, nil
}
