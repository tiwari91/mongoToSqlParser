package sqlconverter

import "fmt"

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
