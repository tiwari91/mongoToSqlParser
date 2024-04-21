package main

import (
	"fmt"
	"os"
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

	oplogBytes, err := os.ReadFile("db/input.json")
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON := string(oplogBytes)

	//fmt.Println("oplogJSON", oplogJSON)

	service.ProcessLogFile(db, oplogJSON)

	endTime := time.Since(start)

	fmt.Println("time took for processing:", endTime)
}
