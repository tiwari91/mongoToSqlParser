package main

import (
	"fmt"
	"mongotosqlparser/sqlconverter"
	"os"
)

func main() {

	fmt.Println("\n-------Insert-------")
	//insert
	oplogBytes, err := os.ReadFile("db/sampleFileInsert.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON := string(oplogBytes)
	//fmt.Println("The File is opened successfully...", oplogJSON)

	createSchemaSQL, createTableSQL, insertSQL, err := sqlconverter.ConvertToSQLInsert(oplogJSON)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("SQL createSchemaSQL:", createSchemaSQL)
	fmt.Println("\nSQL createTableSQL:", createTableSQL)
	fmt.Println("\nSQL Statement Insert:", insertSQL)

	fmt.Println("\n-------Update-------")

	// update
	oplogBytes, err = os.ReadFile("db/sampleFileUpdate.json")
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

	fmt.Println("\nSQL Statement update:", sqlStatement)

	fmt.Println("\n-------Delete-------")

	oplogBytes, err = os.ReadFile("db/sampleFileDelete.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON = string(oplogBytes)

	sqlStatement, err = sqlconverter.ConvertToSQLDelete(oplogJSON)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("SQL Statement Delete:", sqlStatement)

}
