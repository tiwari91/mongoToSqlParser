package main

import (
	"flag"
	"fmt"
	"mongotosqlparser/mongotosqlparser/sqlconverter"

	"os"
)

func main() {

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

	sqlconverter.ProcessLogFile(oplogJSON, *outputFilename)
}
