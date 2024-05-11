package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

const ChunkSize = 500 // Adjust based on your needs and memory constraints

// TableOperation is a function type that represents a table transformation.
type TableOperation func(data [][]string, startIndex int) [][]string

// Pipeline structure holds the input filename and a list of operations to execute.
type Pipeline struct {
	filePath   string
	operations []TableOperation
	chunkSize  int
}

// Read initializes a pipeline with the given input file.
func Read(filePath string) *Pipeline {
	return &Pipeline{
		filePath:   filePath,
		operations: []TableOperation{},
		chunkSize:  ChunkSize,
	}
}

// Add an operation to the pipeline.
func (p *Pipeline) With(operation TableOperation) *Pipeline {
	p.operations = append(p.operations, operation)
	return p
}

// Write the processed data to an output file, skipping empty rows.
func (p *Pipeline) Write(outputPath string) error {
	inputFile, err := os.Open(p.filePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	var wg sync.WaitGroup
	tempFiles := []string{} // to store the names of temporary files in order
	globalRowIndex := 0

	// Process data in chunks and write each chunk to a temporary file
	for idx := 0; ; idx++ {
		chunk, err := readChunk(reader, p.chunkSize)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		tempFileName := fmt.Sprintf("temp_%d.csv", idx) // Sequentially named file
		tempFile, err := os.Create(tempFileName)
		fmt.Printf("Created temporary file %s\n", tempFileName)
		if err != nil {
			return err
		}
		tempFiles = append(tempFiles, tempFileName)

		wg.Add(1)
		chunkStartIndex := globalRowIndex // Capture the start index for this chunk
		go func(file *os.File, chunk [][]string, startIndex int) {
			defer wg.Done()
			defer file.Close()

			for _, op := range p.operations {
				chunk = op(chunk, startIndex)
			}
			writer := csv.NewWriter(file)
			// Write non-empty records to the temporary file
			for _, record := range chunk {
				if !isEmptyRow(record) { // Check if the row is empty
					if err := writer.Write(record); err != nil {
						fmt.Println("Error writing to temp file:", err)
						break
					}
				}
			}
			writer.Flush()
		}(tempFile, chunk, chunkStartIndex)
		globalRowIndex += len(chunk)
	}

	wg.Wait()

	// Merge all temporary files into a single output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	for _, tempFile := range tempFiles {
		if err := mergeTempFile(writer, tempFile); err != nil {
			fmt.Println("err occurred when merging temp file")
			return err
		}
		fmt.Println("Removing temporary file " + tempFile)
		os.Remove(tempFile) // Remove temporary file after merging
	}
	writer.Flush()

	return nil
}

// isEmptyRow checks if a CSV row is empty.
func isEmptyRow(row []string) bool {
	for _, cell := range row {
		if cell != "" {
			return false
		}
	}
	return true
}

func mergeTempFile(writer *csv.Writer, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}

// Read a specific chunk from the CSV reader.
func readChunk(reader *csv.Reader, chunkSize int) ([][]string, error) {
	var chunk [][]string
	for i := 0; i < chunkSize; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			return chunk, io.EOF
		} else if err != nil {
			return nil, err
		}
		chunk = append(chunk, record)
	}
	return chunk, nil
}

// ForEveryColumn returns a table operation that applies a function to each cell in each row.
func ForEveryColumn(transform func(string) string) TableOperation {
	return func(data [][]string, index int) [][]string {
		for i := range data {
			for j := range data[i] {
				data[i][j] = transform(data[i][j])
			}
		}
		return data
	}
}

// GetColumnRange filters a table to include only the columns within the specified range [start, end).
func GetColumns(start, end int) TableOperation {
	return func(data [][]string, index int) [][]string {
		var result [][]string
		for _, row := range data {
			// Ensure that the start and end indices are within the row length
			if start < 0 || start >= len(row) {
				// If the start is out of bounds, consider it an empty row
				result = append(result, []string{})
				continue
			}
			if end > len(row) {
				end = len(row) // Limit end to the last index of the row
			}
			if end < start {
				result = append(result, []string{})
				continue
			}
			// Extract the sublist from start to end
			newRow := row[start:end]
			result = append(result, newRow)
		}
		return result
	}
}

// GetRows filters rows within a specified range.
func GetRows(start, end int) TableOperation {
	return func(data [][]string, startIndex int) [][]string {
		var result [][]string
		for i, row := range data {
			globalIndex := startIndex + i
			if globalIndex >= start && globalIndex < end {
				result = append(result, row)
			}
		}
		return result
	}
}

// SumRow returns a table operation that sums each column's values into a single row.
func SumRow() TableOperation {
	return func(data [][]string, index int) [][]string {
		if len(data) == 0 {
			return [][]string{}
		}
		sums := make([]int, len(data[0]))
		for _, row := range data {
			for i, cell := range row {
				val, err := strconv.Atoi(cell)
				if err == nil {
					sums[i] += val
				}
			}
		}
		sumRow := make([]string, len(sums))
		for i, sum := range sums {
			sumRow[i] = strconv.Itoa(sum)
		}
		return [][]string{sumRow}
	}
}

func main() {
	Read("main/input.csv").
		With(ForEveryColumn(func(cell string) string {
			n, _ := strconv.Atoi(cell)
			return strconv.Itoa(n * 2)
		})).
		With(GetColumns(10, 15)).
		With(GetRows(1, 20)).
		//With(SumRow()).
		Write("output.csv")

	//Read("main/input.csv").
	//	With(GetColumns(1, 4)).
	//	With(GetRows(1, 5)).
	//	Write("output.csv")
}

//Read("input.csv").With(ForEveryColumn(func(cell string) string {
//	n, _ := strconv.Atoi(cell)
//	return strconv.Itoa(n * 2)
//})).With(GetColumns(3, 5)).With(GetRows(7, 20)).With(SumRow()).Write("output.csv")
