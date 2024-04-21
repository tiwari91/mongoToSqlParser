package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/tiwari91/mongoparser/internal/domain"
)

func worker(db *sql.DB, wg *sync.WaitGroup, oplogs []Oplog, resultChannel chan<- string, existingSchemas map[string]bool,
	createdTables map[string][]string, processedOplogs map[string]bool, processedOplogsMu *sync.Mutex) {

	defer wg.Done()

	processedOplogsMu.Lock()
	defer processedOplogsMu.Unlock()

	for index, oplog := range oplogs {

		exists, err := domain.PositionExists(db, index)
		if err != nil {
			resultChannel <- fmt.Sprintf("Error checking position existence: %v", err)
			continue
		}
		if exists {
			continue
		}

		err = domain.SavePosition(db, index)
		if err != nil {
			resultChannel <- fmt.Sprintf("Error saving position: %v", err)
			continue
		}

		processedOplogs[strconv.Itoa(index)] = true

		var data map[string]interface{}
		err = json.Unmarshal(oplog.O, &data)
		if err != nil {
			resultChannel <- fmt.Sprintf("Error unmarshaling JSON: %s", err)
			continue
		}

		switch oplog.Op {
		case "i":
			err = processInsertOpertion(oplog.Ns, data, existingSchemas, createdTables, resultChannel)
		case "u":
			err = processUpdateOperation(oplog.Ns, oplog.O2.ID, data, resultChannel)
		case "d":
			err = processDeleteOperation(oplog.Ns, data, resultChannel)
		default:
			continue
		}

		if err != nil {
			resultChannel <- fmt.Sprintf("Error: %s", err)
			continue
		}

	}
}
