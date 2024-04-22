package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	// Handle interrupt signals and context cancellation
	ctx := handleInterruptConnection()

	inputFilename := "db/input.json"
	outputFilename := "db/result.sql"

	service.ProcessLogFile(db, inputFilename, outputFilename)

	endTime := time.Since(start)
	fmt.Println("\nTime took for processing:", endTime)

	<-ctx.Done()
}

func handleInterruptConnection() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-signalChan
		cancel()
	}()

	return ctx
}
