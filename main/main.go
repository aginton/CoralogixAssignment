package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Processor interface {
	ProcessRow(row []string, rowIndex int) ([]string, error)
	With(Processor) Processor
	Next() Processor
}

type BaseProcessor struct {
	next Processor
}

func (bp *BaseProcessor) With(next Processor) Processor {
	bp.next = next
	return next
}

func (bp *BaseProcessor) Next() Processor {
	return bp.next
}

// Processor to handle row selection
type GetRowsProcessor struct {
	BaseProcessor
	start, end, currentRow int
}

func GetRows(start, end int) Processor {
	return &GetRowsProcessor{start: start, end: end}
}

func (p *GetRowsProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	if rowIndex < p.start || rowIndex >= p.end {
		return nil, nil // Return nil if the row index is outside the specified range
	}
	return row, nil
}

// Processor to handle column selection
type GetColumnsProcessor struct {
	BaseProcessor
	start, end int
}

func GetColumns(start, end int) Processor {
	return &GetColumnsProcessor{start: start, end: end}
}

func (p *GetColumnsProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	if p.end > len(row) {
		p.end = len(row)
	}
	if p.start < 0 || p.start >= p.end {
		return nil, fmt.Errorf("column indices are out of range. Start should be between 0 and %d", len(row))
	}
	return row[p.start:p.end], nil
}

// Processor to apply transformation to every column
type ForEveryColumnProcessor struct {
	BaseProcessor
	transformFunc func(string) string
}

func ForEveryColumn(transformFunc func(string) string) Processor {
	return &ForEveryColumnProcessor{transformFunc: transformFunc}
}

func (p *ForEveryColumnProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	for i, value := range row {
		row[i] = p.transformFunc(value)
	}
	return row, nil
}

type SumRowProcessor struct {
	BaseProcessor
	columnIndices []int
}

func SumRow(columnIndices ...int) Processor {
	return &SumRowProcessor{columnIndices: columnIndices}
}

func (p *SumRowProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	if isEffectivelyEmpty(row) {
		return nil, nil
	}
	sum := 0
	if len(p.columnIndices) == 0 { // If no specific column indices are provided
		for _, cell := range row {
			val, err := strconv.Atoi(cell) // Convert each cell from string to integer
			if err != nil {
				fmt.Printf("Non-numeric value found in row %d: %s\n. Using 0 instead", rowIndex, cell)
			} else {
				sum += val // Add the integer value to the running sum
			}
		}
	} else {
		for _, index := range p.columnIndices {
			rowLen := len(row)
			if index < 0 || index >= rowLen {
				return nil, fmt.Errorf("column index %d is out of range for row %d. Should be between 0 and %d\n", index, rowIndex, rowLen)
			}
			val, err := strconv.Atoi(row[index])
			if err != nil {
				fmt.Printf("Non-numeric value found in row %d, column %d: %s. Using 0 instead\n", rowIndex, index, row[index])
			} else {
				sum += val
			}
		}
	}
	return []string{strconv.Itoa(sum)}, nil
}

// RowAvgProcessor calculates the average of specific or all columns
type RowAvgProcessor struct {
	BaseProcessor
	columnIndices []int
}

// RowAvg creates a new processor for calculating averages
func RowAvg(columnIndices ...int) Processor {
	return &RowAvgProcessor{columnIndices: columnIndices}
}

// ProcessRow calculates the average of specified or all columns
func (p *RowAvgProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	sum := 0
	count := 0

	if len(p.columnIndices) == 0 {
		// Calculate the average for all columns
		for _, value := range row {
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("non-numeric value found in row %d", rowIndex)
			}
			sum += n
			count++
		}
	} else {
		// Calculate the average for specified columns only
		for _, index := range p.columnIndices {
			if index < 0 || index >= len(row) {
				return nil, fmt.Errorf("column index %d is out of range for row %d", index, rowIndex)
			}
			n, err := strconv.Atoi(row[index])
			if err != nil {
				return nil, fmt.Errorf("non-numeric value found at column %d in row %d", index, rowIndex)
			}
			sum += n
			count++
		}
	}

	// If no columns or rows are considered, avoid division by zero
	if count == 0 {
		return nil, fmt.Errorf("no numeric data to compute average in row %d", rowIndex)
	}

	average := float64(sum) / float64(count)
	return []string{fmt.Sprintf("%.2f", average)}, nil
}

// Pipeline to connect and process all processors
type DataPipeline struct {
	BaseProcessor
	filePath   string
	processors Processor
}

func Read(filePath string) *DataPipeline {
	return &DataPipeline{filePath: filePath}
}

func (dp *DataPipeline) With(p Processor) *DataPipeline {
	if dp.processors == nil {
		dp.processors = p
	} else {
		// Find the last processor in the chain
		last := dp.processors
		for last.Next() != nil {
			last = last.Next()
		}
		last.With(p)
	}
	return dp
}

func (dp *DataPipeline) Write(outputFilePath string) error {
	file, err := os.Open(dp.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	rowIndex := 0
	for {
		inputRow, err := reader.Read()
		if err == io.EOF {
			fmt.Printf("Arrived at end of file at row %d\n", rowIndex)
			break
		} else if err != nil {
			return err
		}

		currentRow := inputRow
		fmt.Printf("Before processing: Row %d: %v\n", rowIndex, inputRow)

		for proc := dp.processors; proc != nil; proc = proc.Next() {
			currentRow, err = proc.ProcessRow(currentRow, rowIndex)
			if err != nil {
				return err
			}
			fmt.Printf("After processing with %T: Row %d: %v\n", proc, rowIndex, currentRow)
			if currentRow == nil {
				fmt.Printf("Row %d resulted in nil and was skipped\n", rowIndex)
				break
			}
		}

		if !isEffectivelyEmpty(currentRow) {
			if err := writer.Write(currentRow); err != nil {
				return fmt.Errorf("failed to write row %d: %v", rowIndex, err)
			}
			fmt.Printf("Written to file: Row %d: %v\n", rowIndex, currentRow)
		}

		rowIndex++
	}

	return nil
}

func isEffectivelyEmpty(row []string) bool {
	if len(row) == 0 {
		return true
	}
	if !isEmptyRow(row) {
		return false
	}
	return true
}

func isEmptyRow(row []string) bool {
	for _, cell := range row {
		if cell != "" {
			return false
		}
	}
	return true
}

func main() {
	// Open (or create) a log file for writing logs
	//logFile, err := os.OpenFile("console_output.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	//if err != nil {
	//	fmt.Println("Error opening log file:", err)
	//	return
	//}
	//defer logFile.Close()
	//
	//// Redirect standard output and standard error to the log file
	//os.Stdout = logFile
	//os.Stderr = logFile

	//err2 := Read("main/input.csv").
	//	With(GetRows(862, 30000)).
	//	With(GetColumns(1, 5)).
	//	With(ForEveryColumn(func(cell string) string {
	//		n, err := strconv.Atoi(cell)
	//		if err != nil {
	//			return cell // Returning empty string or some default value in case of error
	//		}
	//		return strconv.Itoa(n * 2)
	//	})).
	//	Write("output.csv")
	//
	//if err2 != nil {
	//	fmt.Println("Error:", err2)
	//}

	//err2 := Read("main/input.csv").
	//	With(GetRows(1, 5)).
	//	With(GetColumns(10, 50)).
	//	//With(ForEveryColumn(func(cell string) string {
	//	//	n, err := strconv.Atoi(cell)
	//	//	if err != nil {
	//	//		return cell // Returning empty string or some default value in case of error
	//	//	}
	//	//	return strconv.Itoa(n * 2)
	//	//})).
	//	//With(SumRow()).
	//	With(RowAvg()).
	//	Write("output.csv")

	err2 := Read("main/input.csv").
		With(GetRows(1, 5)).
		With(GetColumns(10, 50)).
		With(RowAvg()).
		Write("output.csv")

	if err2 != nil {
		fmt.Println("Error:", err2)
	}
}
