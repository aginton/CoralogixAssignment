package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

const ChunkSize = 500

type OperationType int

const (
	FilterOperation OperationType = iota
	TransformOperation
)

type Operation struct {
	OpType  OperationType
	Execute func(data [][]string, rowOffset int) ([][]string, error)
}

type Pipeline struct {
	filePath        string
	filters         []Operation
	transformations []Operation
	chunkSize       int
	tempFiles       []string
}

func Read(filePath string) *Pipeline {
	return &Pipeline{
		filePath:        filePath,
		filters:         []Operation{},
		transformations: []Operation{},
		chunkSize:       ChunkSize,
	}
}

// Add an operation to the pipeline.
func (p *Pipeline) With(operation Operation) *Pipeline {
	if operation.OpType == FilterOperation {
		p.filters = append(p.filters, operation)
	} else {
		p.transformations = append(p.transformations, operation)
	}
	return p
}

// Write the processed data to an output file, skipping empty rows.
func (p *Pipeline) Write(outputPath string) error {
	tempFilteredFile := "temp_filtered.csv"
	err := p.applyFilters(tempFilteredFile)
	if err != nil {
		return err
	}

	err = p.applyTransformations(tempFilteredFile, outputPath)
	if err != nil {
		return err
	}

	os.Remove(tempFilteredFile)
	return nil
}

// Helper function to apply a sequence of operations and write the result to final file using temporary files
func processOperationsInChunksAndWriteToFile(
	inputFilePath string,
	outputFilePath string,
	chunkSize int,
	operations []Operation,
	tempFilePrefix string) error {

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	reader := csv.NewReader(inputFile)
	var wg sync.WaitGroup
	tempFiles := []string{}
	globalRowIndex := 0
	sequence := 0

	for {
		// Read a chunk of data
		chunk, readErr := readChunk(reader, chunkSize)
		originalChunkLen := len(chunk)
		if len(chunk) > 0 {
			// Apply each operation in the sequence
			for _, operation := range operations {
				var opErr error
				chunk, opErr = operation.Execute(chunk, globalRowIndex)
				if opErr != nil {
					return opErr
				}
			}
			globalRowIndex += originalChunkLen

			// Write the processed chunk to a temporary file if not empty
			if len(chunk) > 0 {
				tempFileName := fmt.Sprintf("%s_chunk_%d.csv", tempFilePrefix, sequence)
				tempFiles = append(tempFiles, tempFileName)
				sequence++

				tempFile, err := os.Create(tempFileName)
				if err != nil {
					return err
				}

				wg.Add(1)
				go func(file *os.File, data [][]string) {
					defer wg.Done()
					defer file.Close()
					writeChunk(file, data)
				}(tempFile, chunk)
			}
		}

		if readErr == io.EOF {
			break
		} else if readErr != nil {
			return readErr
		}
	}

	wg.Wait()

	// Merge all temporary files into the final output file
	return mergeAndRemoveTemporaryFiles(tempFiles, outputFilePath)
}

func (p *Pipeline) applyFilters(outputFilePath string) error {
	return processOperationsInChunksAndWriteToFile(
		p.filePath,     // Use input file path from the pipeline
		outputFilePath, // Output file path
		p.chunkSize,    // Chunk size
		p.filters,      // Operations to apply (filters)
		"filtered")     // Prefix for temporary files
}

func (p *Pipeline) applyTransformations(inputFilePath, outputFilePath string) error {
	return processOperationsInChunksAndWriteToFile(
		inputFilePath,     // Use provided input file path
		outputFilePath,    // Output file path
		p.chunkSize,       // Chunk size
		p.transformations, // Operations to apply (transformations)
		"transformed")     // Prefix for temporary files
}

func mergeAndRemoveTemporaryFiles(tempFiles []string, finalFilePath string) error {
	outputFile, err := os.Create(finalFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	for _, tempFile := range tempFiles {
		inputFile, err := os.Open(tempFile)
		if err != nil {
			return err
		}
		reader := csv.NewReader(inputFile)

		for {
			record, err := reader.Read()
			if err == io.EOF {
				inputFile.Close()
				break // Finish reading this temporary file
			} else if err != nil {
				inputFile.Close()
				return err // Handle read errors
			}
			writer.Write(record) // Write each record to the final output file
		}
		os.Remove(tempFile) // Clean up temporary files
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

func writeChunk(file *os.File, data [][]string) {
	writer := csv.NewWriter(file)
	for _, record := range data {
		if !isEmptyRow(record) {
			writer.Write(record)
		}
	}
	writer.Flush()
}

func ForEveryColumn(transformFunc func(string) string) Operation {
	return Operation{
		OpType: TransformOperation,
		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
			for i, row := range data {
				for j, cell := range row {
					data[i][j] = transformFunc(cell)
				}
			}
			return data, nil
		},
	}
}

// GetColumnRange filters a table to include only the columns within the specified range [start, end).
func GetColumns(start, end int) Operation {
	return Operation{
		OpType: FilterOperation,
		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
			var result [][]string
			for _, row := range data {
				if start < 0 || start >= len(row) || end < start {
					result = append(result, []string{})
					continue
				}
				if end > len(row) {
					end = len(row)
				}
				newRow := row[start:end]
				result = append(result, newRow)
			}
			return result, nil
		},
	}
}

func GetRows(start, end int) Operation {
	return Operation{
		OpType: FilterOperation,
		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
			if start < 0 {
				return nil, fmt.Errorf("start index cannot be less than 0")
			}

			// Determine the effective start and end indices within the chunk
			effectiveStart := max(start-rowOffset, 0)     // Adjust start relative to the offset
			effectiveEnd := min(end-rowOffset, len(data)) // Adjust end relative to the offset and limit to chunk size

			// If the indices are valid and there is a range to return
			if effectiveStart < effectiveEnd {
				return data[effectiveStart:effectiveEnd], nil
			}

			// Return an empty slice if out of bounds
			return [][]string{}, nil
		},
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SumRow returns a table operation that sums each column's values into a single row.
func SumRow() Operation {
	return Operation{OpType: TransformOperation,
		Execute: func(data [][]string, startIndex int) ([][]string, error) {
			if len(data) == 0 {
				return [][]string{}, nil
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
			return [][]string{sumRow}, nil
		}}
}

func main() {
	Read("main/input.csv").
		With(GetRows(1, 5)).
		With(GetColumns(2, 5)).
		With(ForEveryColumn(func(cell string) string {
			n, err := strconv.Atoi(cell)
			if err != nil {
				// Handle the error properly, perhaps log it or use a default value
				fmt.Println("Error converting cell ", cell)
				return cell // Returning empty string or some default value in case of error
			}
			return strings.Repeat("h", n) // Correctly repeat "h" n times

		})).
		Write("output.csv")
}
