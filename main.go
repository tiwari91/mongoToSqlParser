package main

import (
	"fmt"
	"mongotosqlparser/sqlconverter"
	"os"
)

func main() {
	//insert
	oplogBytes, err := os.ReadFile("db/sampleFileInsert.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON := string(oplogBytes)
	//fmt.Println("The File is opened successfully...", oplogJSON)

	sqlStatement, err := sqlconverter.ConvertToSQLInsert(oplogJSON)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("SQL Statement:", sqlStatement)

	// update
	oplogBytes, err = os.ReadFile("db/sampleFileUpdate.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON = string(oplogBytes)

	sqlStatement, err = sqlconverter.ConvertToSQLUpdate([]byte(oplogJSON))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("SQL Statement:", sqlStatement)

}
