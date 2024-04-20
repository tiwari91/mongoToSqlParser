package writer

import "os"

func WriterFile(outputFilename string, resultChannel chan string) error {
	file, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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
