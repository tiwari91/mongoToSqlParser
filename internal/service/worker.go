package service

// import (
// 	"database/sql"
// 	"encoding/json"
// 	"fmt"
// 	"sync"

// 	"github.com/tiwari91/mongoparser/internal/domain"
// )

// func worker(db *sql.DB, wg *sync.WaitGroup,
// 	inputChannel <-chan Oplog,
// 	outputChannel chan<- string,
// 	existingSchemas map[string]bool,
// 	createdTables map[string][]string) {

// 	defer wg.Done()

// 	index := 0
// 	for oplog := range inputChannel {
// 		// index := oplog.Index

// 		exists, err := domain.PositionExists(db, index)
// 		if err != nil {
// 			outputChannel <- fmt.Sprintf("Error checking position existence: %v", err)
// 			continue
// 		}
// 		if exists {
// 			continue
// 		}

// 		err = domain.SavePosition(db, index)
// 		if err != nil {
// 			outputChannel <- fmt.Sprintf("Error saving position: %v", err)
// 			continue
// 		}

// 		// processedOplogsMu.Lock()
// 		//processedOplogs[strconv.Itoa(index)] = true
// 		// processedOplogsMu.Unlock()

// 		var data map[string]interface{}
// 		err = json.Unmarshal(oplog.O, &data)
// 		if err != nil {
// 			outputChannel <- fmt.Sprintf("Error unmarshaling JSON: %s", err)
// 			continue
// 		}

// 		switch oplog.Op {
// 		case "i":
// 			err = processInsertOperation(oplog.Ns, data, existingSchemas, createdTables, outputChannel)
// 		case "u":
// 			err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data, outputChannel)
// 		case "d":
// 			err = processDeleteOperation(oplog.Ns, data, outputChannel)
// 		default:
// 			continue
// 		}

// 		if err != nil {
// 			outputChannel <- fmt.Sprintf("Error: %s", err)
// 			continue
// 		}

// 		index++

// 	}
// }
