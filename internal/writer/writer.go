package writer

import (
	"bufio"
	"os"
)

func WriterStreamFile(outputFile *os.File, statement string) error {

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	_, err := writer.WriteString(statement + "\n")
	if err != nil {
		return err
	}

	return nil
}
