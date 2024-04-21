package main

import (
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/tiwari91/mongoparser/db"
	"github.com/tiwari91/mongoparser/internal/service"
)

func main() {

	start := time.Now()

	db, err := db.ConnectDB()
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}
	defer db.Close()

	inputFilename := "db/input.json"
	outputFilename := "db/result.sql"

	service.ProcessLogFile(db, inputFilename, outputFilename)

	endTime := time.Since(start)

	fmt.Println("time took for processing:", endTime)
}
