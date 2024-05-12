package main

//import (
//	"encoding/csv"
//	"fmt"
//	"io"
//	"os"
//	"sort"
//	"strconv"
//	"strings"
//	"sync"
//)
//
//const (
//	ChunkSize  = 500
//	MaxWorkers = 10
//)
//
//type OperationType int
//
//const (
//	FilterOperation OperationType = iota
//	TransformOperation
//)
//
//type Operation struct {
//	OpType  OperationType
//	Execute func(data [][]string, rowOffset int) ([][]string, error)
//}
//
//type Pipeline struct {
//	filePath        string
//	filters         []Operation
//	transformations []Operation
//	chunkSize       int
//	tempFiles       []string
//}
//
//func Read(filePath string) *Pipeline {
//	return &Pipeline{
//		filePath:        filePath,
//		filters:         []Operation{},
//		transformations: []Operation{},
//		chunkSize:       ChunkSize,
//	}
//}
//
//// Add an operation to the pipeline.
//func (p *Pipeline) With(operation Operation) *Pipeline {
//	if operation.OpType == FilterOperation {
//		p.filters = append(p.filters, operation)
//	} else {
//		p.transformations = append(p.transformations, operation)
//	}
//	return p
//}
//
//// Write the processed data to an output file, skipping empty rows.
//func (p *Pipeline) Write(outputPath string) error {
//	tempFilteredFile := "temp_filtered.csv"
//	err := p.applyFilters(tempFilteredFile)
//	if err != nil {
//		return err
//	}
//
//	err = p.applyTransformations(tempFilteredFile, outputPath)
//	if err != nil {
//		return err
//	}
//
//	os.Remove(tempFilteredFile)
//	return nil
//}
//
//// Helper function to apply a sequence of operations and write the result to final file using temporary files
//func processOperationsInChunksAndWriteToFile(
//	inputFilePath string,
//	outputFilePath string,
//	chunkSize int,
//	operations []Operation,
//	tempFilePrefix string) error {
//
//	inputFile, err := os.Open(inputFilePath)
//	if err != nil {
//		return err
//	}
//	defer inputFile.Close()
//
//	reader := csv.NewReader(inputFile)
//	wg := sync.WaitGroup{}
//	workerSemaphore := make(chan struct{}, MaxWorkers)
//	tempFiles := []string{}
//	globalRowIndex := 0
//	sequence := 0
//	var mu sync.Mutex // Mutex to protect the tempFiles slice and sequence number
//
//	for {
//		// Read a chunk of data
//		chunk, readErr := readChunk(reader, chunkSize)
//		if readErr != nil && readErr != io.EOF {
//			return readErr
//		}
//		originalChunkLen := len(chunk)
//		if originalChunkLen == 0 {
//			break
//		}
//		wg.Add(1)
//		workerSemaphore <- struct{}{}
//		localSequence := sequence // Capture the current sequence number for the goroutine
//
//		go func(chunk [][]string, rowIndex int, seq int) {
//			defer wg.Done()
//			defer func() { <-workerSemaphore }()
//
//			for _, op := range operations {
//				var opErr error
//				chunk, opErr = op.Execute(chunk, rowIndex)
//				if opErr != nil {
//					fmt.Println("Error processing chunk:", opErr) // Error handling can be improved
//					return
//				}
//			}
//
//			if !isChunkEffectivelyEmpty(chunk) {
//				tempFileName := fmt.Sprintf("%s_chunk_%d.csv", tempFilePrefix, seq)
//				tempFiles = append(tempFiles, tempFileName)
//				tempFile, err := os.Create(tempFileName)
//				if err != nil {
//					fmt.Println("Error creating temp file", err)
//					return
//				}
//				writeChunk(tempFile, chunk)
//				tempFile.Close()
//			} else {
//				fmt.Printf("Chunk %d is effectively empty, skipping file creation.\n", seq)
//			}
//		}(chunk, globalRowIndex, localSequence)
//
//		mu.Lock()
//		sequence++
//		mu.Unlock()
//		globalRowIndex += originalChunkLen
//		if readErr == io.EOF {
//			break
//		}
//	}
//
//	wg.Wait()
//
//	// Merge all temporary files into the final output file
//	return mergeAndRemoveTemporaryFiles(tempFiles, outputFilePath)
//}
//
//func (p *Pipeline) applyFilters(outputFilePath string) error {
//	return processOperationsInChunksAndWriteToFile(
//		p.filePath,     // Use input file path from the pipeline
//		outputFilePath, // Output file path
//		p.chunkSize,    // Chunk size
//		p.filters,      // Operations to apply (filters)
//		"filtered")     // Prefix for temporary files
//}
//
//func (p *Pipeline) applyTransformations(inputFilePath, outputFilePath string) error {
//	return processOperationsInChunksAndWriteToFile(
//		inputFilePath,     // Use provided input file path
//		outputFilePath,    // Output file path
//		p.chunkSize,       // Chunk size
//		p.transformations, // Operations to apply (transformations)
//		"transformed")     // Prefix for temporary files
//}
//
//func mergeAndRemoveTemporaryFiles(tempFiles []string, finalFilePath string) error {
//	sortByChunkNumber(tempFiles)
//	outputFile, err := os.Create(finalFilePath)
//	if err != nil {
//		return err
//	}
//	defer outputFile.Close()
//
//	writer := csv.NewWriter(outputFile)
//	for _, tempFile := range tempFiles {
//		inputFile, err := os.Open(tempFile)
//		if err != nil {
//			return err
//		}
//		reader := csv.NewReader(inputFile)
//
//		for {
//			record, err := reader.Read()
//			if err == io.EOF {
//				inputFile.Close()
//				break // Finish reading this temporary file
//			} else if err != nil {
//				inputFile.Close()
//				return err // Handle read errors
//			}
//			writer.Write(record) // Write each record to the final output file
//		}
//		os.Remove(tempFile) // Clean up temporary files
//	}
//	writer.Flush()
//	return nil
//}
//
//func sortByChunkNumber(tempFiles []string) {
//	sort.Slice(tempFiles, func(i, j int) bool {
//		// Extract the numeric suffix from the file names
//		getChunkNumber := func(filename string) int {
//			parts := strings.Split(strings.TrimSuffix(filename, ".csv"), "_")
//			// The chunk number should be the last element in parts after splitting
//			number, err := strconv.Atoi(parts[len(parts)-1]) // Convert the last part to a number
//			if err != nil {
//				return -1
//			}
//			return number
//		}
//
//		// Compare based on the numeric value
//		return getChunkNumber(tempFiles[i]) < getChunkNumber(tempFiles[j])
//	})
//}
//
//func isChunkEffectivelyEmpty(data [][]string) bool {
//	if len(data) == 0 {
//		return true
//	}
//	for _, row := range data {
//		if !isEmptyRow(row) {
//			return false
//		}
//	}
//	return true
//}
//
//// isEmptyRow checks if a CSV row is empty.
//func isEmptyRow(row []string) bool {
//	for _, cell := range row {
//		if cell != "" {
//			return false
//		}
//	}
//	return true
//}
//
//// Read a specific chunk from the CSV reader.
//func readChunk(reader *csv.Reader, chunkSize int) ([][]string, error) {
//	var chunk [][]string
//	for i := 0; i < chunkSize; i++ {
//		record, err := reader.Read()
//		if err == io.EOF {
//			return chunk, io.EOF
//		} else if err != nil {
//			return nil, err
//		}
//		chunk = append(chunk, record)
//	}
//	return chunk, nil
//}
//
//func writeChunk(file *os.File, data [][]string) {
//	writer := csv.NewWriter(file)
//	count := 0
//	for _, record := range data {
//		if !isEmptyRow(record) {
//			err := writer.Write(record)
//			if err != nil {
//				fmt.Printf("Failed to write record: %v, Error: %v\n", record, err)
//				continue // Optionally continue to try to write other records or return an error
//			}
//			count++ // Increment count of successfully written records
//		}
//	}
//	writer.Flush()
//	if err := writer.Error(); err != nil {
//		fmt.Printf("Error while flushing writer: %s\n", err)
//	} else {
//		fmt.Printf("Successfully wrote %d records to file %s\n", count, file.Name())
//	}
//}
//
//func ForEveryColumn(transformFunc func(string) string) Operation {
//	return Operation{
//		OpType: TransformOperation,
//		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
//			for i, row := range data {
//				for j, cell := range row {
//					data[i][j] = transformFunc(cell)
//				}
//			}
//			return data, nil
//		},
//	}
//}
//
//// GetColumnRange filters a table to include only the columns within the specified range [start, end).
//func GetColumns(start, end int) Operation {
//	return Operation{
//		OpType: FilterOperation,
//		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
//			var result [][]string
//			for _, row := range data {
//				if start < 0 || end < start {
//					return nil, fmt.Errorf("getColumn invalid indices: start should be greater than 0 and less than end")
//				}
//				if end > len(row) {
//					end = len(row)
//				}
//				newRow := row[start:end]
//				result = append(result, newRow)
//			}
//			return result, nil
//		},
//	}
//}
//
//func GetRows(start, end int) Operation {
//	return Operation{
//		OpType: FilterOperation,
//		Execute: func(data [][]string, rowOffset int) ([][]string, error) {
//			if start < 0 {
//				return nil, fmt.Errorf("start index cannot be less than 0")
//			}
//
//			// Calculate actual end based on data length if it exceeds
//			actualEnd := min(end, rowOffset+len(data))
//
//			// If the requested range is entirely outside of this chunk
//			if rowOffset >= actualEnd || (rowOffset+len(data)) <= start {
//				return [][]string{}, nil
//			}
//
//			// Adjust the indices relative to the current chunk
//			startIndexLocal := max(start-rowOffset, 0)
//			endIndexLocal := min(actualEnd-rowOffset, len(data))
//
//			// Ensure valid range and return the slice
//			if startIndexLocal < endIndexLocal {
//				return data[startIndexLocal:endIndexLocal], nil
//			}
//
//			return [][]string{}, nil
//		},
//	}
//}
//
//func min(a, b int) int {
//	if a < b {
//		return a
//	}
//	return b
//}
//
//func max(a, b int) int {
//	if a > b {
//		return a
//	}
//	return b
//}
//
//// SumRow returns a table operation that sums each column's values into a single row.
//func SumRow() Operation {
//	return Operation{OpType: TransformOperation,
//		Execute: func(data [][]string, startIndex int) ([][]string, error) {
//			if len(data) == 0 {
//				return [][]string{}, nil
//			}
//			sums := make([]int, len(data[0]))
//			for _, row := range data {
//				for i, cell := range row {
//					val, err := strconv.Atoi(cell)
//					if err == nil {
//						sums[i] += val
//					}
//				}
//			}
//			sumRow := make([]string, len(sums))
//			for i, sum := range sums {
//				sumRow[i] = strconv.Itoa(sum)
//			}
//			return [][]string{sumRow}, nil
//		}}
//}
//
//func main() {
//	Read("main/input.csv").
//		With(GetRows(1, 50)).
//		With(GetColumns(2, 6)).
//		With(ForEveryColumn(func(cell string) string {
//			n, err := strconv.Atoi(cell)
//			if err != nil {
//				return cell // Returning empty string or some default value in case of error
//			}
//			return strconv.Itoa(n * 2)
//		})).
//		Write("output.csv")
//}
