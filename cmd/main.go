package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/tiwari91/mongoparser/db"
	"github.com/tiwari91/mongoparser/internal/service"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "bookmarks"
	password = "pa55word"
	dbname   = "bookmarks"
)

func main() {

	start := time.Now()

	db, err := db.ConnectDB()
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}
	defer db.Close()

	inputFilename := flag.String("input", "", "Input filename containing oplogs")
	outputFilename := flag.String("output", "", "Output filename to write SQL statements")
	flag.Parse()

	// Check if input filename is provided
	if *inputFilename == "" {
		fmt.Println("Error: Input filename not provided")
		return
	}

	oplogBytes, err := os.ReadFile(*inputFilename)
	if err != nil {
		fmt.Println("Error reading oplog file:", err)
		return
	}

	oplogJSON := string(oplogBytes)

	//fmt.Println("oplogJSON", oplogJSON)

	service.ProcessLogFile(db, oplogJSON, *outputFilename)

	endTime := time.Since(start)

	fmt.Println("time took for processing:", endTime)
}
