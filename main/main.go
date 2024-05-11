package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"sync"
)

const ChunkSize = 500 // Adjust based on your needs and memory constraints

// TableOperation is a function type that represents a table transformation.
type TableOperation func([][]string) [][]string

// Pipeline structure holds the input filename and a list of operations to execute.
type Pipeline struct {
	operations []TableOperation
	filename   string
}

// Read initializes a pipeline with the given input file.
func Read(filename string) *Pipeline {
	return &Pipeline{filename: filename}
}

// Add an operation to the pipeline.
func (p *Pipeline) With(operation TableOperation) *Pipeline {
	p.operations = append(p.operations, operation)
	return p
}

// Execute the operations in the pipeline and return the processed data.
func (p *Pipeline) execute() [][]string {
	file, err := os.Open(p.filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var data [][]string
	var wg sync.WaitGroup
	resultChan := make(chan [][]string, 10) // buffer size depends on concurrency level

	// Process chunks concurrently.
	for {
		chunk, err := readChunk(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		wg.Add(1)
		go func(chunk [][]string) {
			defer wg.Done()
			for _, op := range p.operations {
				chunk = op(chunk)
			}
			resultChan <- chunk
		}(chunk)
	}

	// Close the result channel once all chunks are processed.
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect all processed chunks into final data.
	for chunk := range resultChan {
		data = append(data, chunk...)
	}

	return data
}

// Write the processed data to an output file, skipping empty rows.
func (p *Pipeline) Write(outputFile string) {
	data := p.execute()

	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Iterate over each record and write only non-empty records.
	for _, record := range data {
		if !isEmptyRow(record) {
			if err := writer.Write(record); err != nil {
				panic(err) // handle or log error appropriately
			}
		}
	}
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

// Read a specific chunk from the CSV reader.
func readChunk(reader *csv.Reader) ([][]string, error) {
	var chunk [][]string
	for i := 0; i < ChunkSize; i++ {
		record, err := reader.Read()
		if err != nil {
			return chunk, err
		}
		chunk = append(chunk, record)
	}
	return chunk, nil
}

// ForEveryColumn returns a table operation that applies a function to each cell in each row.
func ForEveryColumn(transform func(string) string) TableOperation {
	return func(data [][]string) [][]string {
		for i := range data {
			for j := range data[i] {
				data[i][j] = transform(data[i][j])
			}
		}
		return data
	}
}

// GetColumns filters a table to include only the specified columns.
func GetColumns(cols ...int) TableOperation {
	return func(data [][]string) [][]string {
		var result [][]string
		for _, row := range data {
			var newRow []string
			for _, col := range cols {
				if col < len(row) {
					newRow = append(newRow, row[col])
				}
			}
			result = append(result, newRow)
		}
		return result
	}
}

// GetRows filters rows within a specified range.
func GetRows(start, end int) TableOperation {
	return func(data [][]string) [][]string {
		if end > len(data) {
			end = len(data)
		}
		if start > end {
			return [][]string{}
		}
		return data[start:end]
	}
}

// SumRow returns a table operation that sums each column's values into a single row.
func SumRow() TableOperation {
	return func(data [][]string) [][]string {
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
	//Read("main/input.csv").
	//	With(ForEveryColumn(func(cell string) string {
	//		n, _ := strconv.Atoi(cell)
	//		return strconv.Itoa(n * 2)
	//	})).
	//	With(GetColumns(3, 5)).
	//	With(GetRows(7, 20)).
	//	With(SumRow()).
	//	Write("output.csv")

	Read("main/input.csv").
		With(GetColumns(3, 5)).
		With(GetRows(7, 20)).
		Write("output.csv")
}

//Read("input.csv").With(ForEveryColumn(func(cell string) string {
//	n, _ := strconv.Atoi(cell)
//	return strconv.Itoa(n * 2)
//})).With(GetColumns(3, 5)).With(GetRows(7, 20)).With(SumRow()).Write("output.csv")
