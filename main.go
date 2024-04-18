package main

import (
	"fmt"
	"mongotosqlparser/sqlconverter"
	"os"
)

func main() {

	fmt.Println("\n-------Insert-------")

	oplogBytes, err := os.ReadFile("db/Input.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON := string(oplogBytes)
	//fmt.Println("The File is opened successfully...", oplogJSON)

	sqlStatements, err := sqlconverter.ConvertToSQLInsert(oplogJSON)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	//fmt.Println("\n\n\nSQL Statement Insert:", sqlStatements)

	err = os.WriteFile("output.sql", []byte(sqlStatements), 0644)
	if err != nil {
		fmt.Println("Error writing SQL statements to file:", err)
		return
	}
	fmt.Println("SQL statements insert written to output.sql successfully")

	// fmt.Println("\n-------Update-------")

	oplogBytes, err = os.ReadFile("db/Input.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON = string(oplogBytes)

	sqlStatement, err := sqlconverter.ConvertToSQLUpdate([]byte(oplogJSON))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	//fmt.Println("\nSQL Statement update:", sqlStatement)
	file, err := os.OpenFile("output.sql", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening output.sql file:", err)
		return
	}
	//	defer file.Close()

	_, err = file.WriteString("\n" + sqlStatement)
	if err != nil {
		fmt.Println("Error appending SQL statements to file:", err)
		return
	}
	fmt.Println("SQL statements update appended to output.sql successfully")

	// fmt.Println("\n-------Delete-------")

	oplogBytes, err = os.ReadFile("db/Input.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON = string(oplogBytes)

	sqlStatement, err = sqlconverter.ConvertToSQLDelete([]byte(oplogJSON))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	//fmt.Println("\nSQL Statement update:", sqlStatement)
	file, err = os.OpenFile("output.sql", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening output.sql file:", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString("\n" + sqlStatement)
	if err != nil {
		fmt.Println("Error appending SQL statements to file:", err)
		return
	}
	fmt.Println("SQL statements delete appended to output.sql successfully")

}
