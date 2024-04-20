package sqlconverter

import (
	"fmt"

	"github.com/google/uuid"
)

func contains(arr []string, item string) bool {
	for _, value := range arr {
		if value == item {
			return true
		}
	}
	return false
}

func getStudentId(data map[string]interface{}) string {
	studentID := ""
	for key, value := range data {
		if key == "_id" {
			studentID = fmt.Sprintf("%v", value)
			break
		}
	}

	return studentID
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

// Function to check if a table exists in the database schema
func tableExists(namespace string, existingSchemas map[string][]string) bool {
	_, exists := existingSchemas[namespace]
	return exists
}

// Function to generate a UUID (randomly generated _id)
func generateUUID() string {
	u := uuid.New()
	return u.String()
}
