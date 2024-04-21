package writer

import (
	"bufio"
	"os"
)

func WriterFile(resultChannel chan string) error {
	file, err := os.OpenFile("db/result.sql", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for result := range resultChannel {
		_, err := file.WriteString(result + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func WriterStreamFile(outputFile *os.File, statement string) error {

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	_, err := writer.WriteString(statement + "\n")
	if err != nil {
		return err
	}

	return nil
}
