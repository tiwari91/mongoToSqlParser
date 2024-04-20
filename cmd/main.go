package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tiwari91/mongoparser/internal/logparser"
)

func main() {

	start := time.Now()

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

	logparser.ProcessLogFile(oplogJSON, *outputFilename)

	endTime := time.Since(start)

	fmt.Println("time took for processing:", endTime)
}
