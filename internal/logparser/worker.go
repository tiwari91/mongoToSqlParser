package logparser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/tiwari91/mongoparser/internal/service"
)

func worker(wg *sync.WaitGroup, oplogs []Oplog, resultChannel chan<- string, existingSchemas map[string]bool,
	createdTables map[string][]string, processedOplogs map[string]bool, processedOplogsMu *sync.Mutex) {

	defer wg.Done()

	processedOplogsMu.Lock()
	defer processedOplogsMu.Unlock()

	for index, oplog := range oplogs {
		if processedOplogs[strconv.Itoa(index)] {
			continue
		}
		processedOplogs[strconv.Itoa(index)] = true

		var data map[string]interface{}
		err := json.Unmarshal(oplog.O, &data)
		if err != nil {
			resultChannel <- fmt.Sprintf("Error unmarshaling JSON: %s", err)
			continue
		}

		switch oplog.Op {
		case "i":
			err = service.ProcessInsert(oplog.Ns, data, existingSchemas, createdTables, resultChannel)
		case "u":
			err = service.ConvertToSQLUpdate(oplog.Ns, oplog.O2.ID, data, resultChannel)
		case "d":
			err = service.ConvertToSQLDelete(oplog.Ns, data, resultChannel)
		default:
			continue
		}

		if err != nil {
			resultChannel <- fmt.Sprintf("Error: %s", err)
			continue
		}
	}
}
