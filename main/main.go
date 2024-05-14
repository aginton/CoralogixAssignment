package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
)

type BaseProcessor struct {
	next Processor
}

type Processor interface {
	ProcessRow(row []string, rowIndex int) ([]string, error)
	With(Processor) Processor
	Next() Processor
	ProcessAggregation(table [][]string) ([][]string, error)
	isAggregator() bool
}

func (bp *BaseProcessor) isAggregator() bool {
	return false
}

func (bp *BaseProcessor) With(next Processor) Processor {
	bp.next = next
	return next
}

func (bp *BaseProcessor) Next() Processor {
	return bp.next
}

// Helper function for ProcessAggregation to be used in processors that don't have custom implementation
func aggregateRows(table [][]string, process func([]string, int) ([]string, error)) ([][]string, error) {
	result := [][]string{}
	for rowIndex, row := range table {
		processedRow, err := process(row, rowIndex)
		if err != nil {
			return nil, err
		}
		if processedRow != nil {
			result = append(result, processedRow)
		}
	}
	return result, nil
}

// //GetRows Processor used for row selection
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

func (p *GetRowsProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	return aggregateRows(table, p.ProcessRow)
}

// GetColumns Processor for column selection
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
		return nil, fmt.Errorf("%T Error: column indices are out of range. Start should be between 0 and less than end", p)
	}
	return row[p.start:p.end], nil
}

func (p *GetColumnsProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	return aggregateRows(table, p.ProcessRow)
}

// Processor to apply transformation to every cell
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

func (p *ForEveryColumnProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	for _, row := range table {
		for i, value := range row {
			row[i] = p.transformFunc(value)
		}
	}
	return table, nil
}

// Processor to get sum across entire row (if no columnIndices are provided), or to get sum for specified columns. Uses 0 for non-numeric values
type SumRowProcessor struct {
	BaseProcessor
	columnIndices []int
}

func SumRow(columnIndices ...int) Processor {
	return &SumRowProcessor{columnIndices: columnIndices}
}

func (p *SumRowProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	if len(row) == 0 {
		return nil, nil
	}
	sum := 0
	if len(p.columnIndices) == 0 { // If no specific column indices are provided
		for _, cell := range row {
			val, err := strconv.Atoi(cell) // Convert each cell from string to integer
			if err != nil {
				return nil, fmt.Errorf("%T Error: non-numeric value found in row %d: %s\n. Using 0 instead", p, rowIndex, cell)
			} else {
				sum += val // Add the integer value to the running sum
			}
		}
	} else {
		for _, index := range p.columnIndices {
			rowLen := len(row)
			if index < 0 || index >= rowLen {
				return nil, fmt.Errorf("%T Error: column index %d is out of range for row %d. Should be between 0 and %d\n", p, index, rowIndex, rowLen)
			}
			val, err := strconv.Atoi(row[index])
			if err != nil {
				fmt.Printf("%T Error: Non-numeric value found in row %d, column %d: %s. Using 0 instead\n", p, rowIndex, index, row[index])
			} else {
				sum += val
			}
		}
	}
	return []string{strconv.Itoa(sum)}, nil
}

func (p *SumRowProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	return aggregateRows(table, p.ProcessRow)
}

// RowAvgProcessor calculates the average of specific row using all columns if columnIndices is blank, otherwise for specified columns
type RowAvgProcessor struct {
	BaseProcessor
	columnIndices []int
}

func RowAvg(columnIndices ...int) Processor {
	return &RowAvgProcessor{columnIndices: columnIndices}
}

// ProcessRow calculates the average of specified or all columns
func (p *RowAvgProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	sum := 0
	count := 0

	if len(p.columnIndices) == 0 {
		// Calculate the average for all columns
		for colIndex, value := range row {
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("%T Error: non-numeric value found in row %d, column %d: %v", p, rowIndex, colIndex, value)
			}
			sum += n
			count++
		}
	} else {
		// Calculate the average for specified columns only
		for _, index := range p.columnIndices {
			if index < 0 || index >= len(row) {
				return nil, fmt.Errorf("%T Error: column index %d is out of range for row %d", p, index, rowIndex)
			}
			n, err := strconv.Atoi(row[index])
			if err != nil {
				return nil, fmt.Errorf("%T Error: non-numeric value found at column %d in row %d", p, index, rowIndex)
			}
			sum += n
			count++
		}
	}

	// If no columns or rows are considered, avoid division by zero
	if count == 0 {
		return nil, fmt.Errorf("%T Error: no numeric data to compute average in row %d", p, rowIndex)
	}

	average := float64(sum) / float64(count)
	return []string{fmt.Sprintf("%.2f", average)}, nil
}

func (p *RowAvgProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	return aggregateRows(table, p.ProcessRow)
}

// AvgProcessor computes the average for specified or all columns.
type AvgProcessor struct {
	BaseProcessor
	columnIndices []int
	sums          []int
	counts        []int
}

func GetAvg(columnIndices ...int) Processor {
	return &AvgProcessor{columnIndices: columnIndices}
}

func (p *AvgProcessor) isAggregator() bool {
	return true
}

func (p *AvgProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	// Initialize sums and counts on the first row if they have not been initialized
	if p.sums == nil || p.counts == nil {
		// Determine the columns to process: either specified columns or all columns
		if len(p.columnIndices) == 0 {
			p.columnIndices = make([]int, len(row)) // If no columns specified, use all columns
			for i := range row {
				p.columnIndices[i] = i
			}
		}
		// Initialize sums and counts based on the size of columnIndices
		p.sums = make([]int, len(p.columnIndices))
		p.counts = make([]int, len(p.columnIndices))
	}

	// Process each specified column
	for i, index := range p.columnIndices {
		if index < 0 || index >= len(row) { // Check index range
			continue // Skip out-of-range indices
		}
		value, err := strconv.Atoi(row[index])
		if err != nil {
			return nil, fmt.Errorf("%T Error: non-numeric value found at column %d in row %d", p, index, rowIndex)
		}
		p.sums[i] += value
		p.counts[i]++
	}

	return row, nil
}

func (p *AvgProcessor) ProcessAggregation([][]string) ([][]string, error) {
	averages := make([]float64, len(p.sums))
	for i, sum := range p.sums {
		if p.counts[i] == 0 {
			averages[i] = 0 // Avoid division by zero
		} else {
			averages[i] = float64(sum) / float64(p.counts[i])
		}
	}
	strings := float64SliceToStringSlice(averages)
	resultTable := make([][]string, 1)
	resultTable[0] = make([]string, len(strings))
	// Populate the first (and only) row with the data from inputRow.
	copy(resultTable[0], strings)
	return resultTable, nil
}

func float64SliceToStringSlice(floats []float64) []string {
	strings := make([]string, len(floats))
	for i, v := range floats {
		strings[i] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strings
}

// TopNProcessor processes the top N rows based on a specified column. It uses a custom comparator
type TopNProcessor struct {
	BaseProcessor
	n           int
	columnIndex int
	topRows     []TopRow
	comparator  func(a, b string) bool
}

func TopN(n int, columnIndex int) *TopNProcessor {
	return &TopNProcessor{
		n:           n,
		columnIndex: columnIndex,
		topRows:     make([]TopRow, 0, n),
		comparator:  compare,
	}
}

func (p *TopNProcessor) isAggregator() bool {
	return true
}

// TopRow represents a row with a comparable value.
type TopRow struct {
	row   []string
	value string
}

func (p *TopNProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	if len(row) <= p.columnIndex {
		return nil, fmt.Errorf("%T error: column index out of range. Should be between 0 and %d", p, len(row)-1)
	}
	val := row[p.columnIndex]
	elementAdded := false
	if len(p.topRows) < p.n {
		p.topRows = append(p.topRows, TopRow{row: row, value: val})
		elementAdded = true
	} else {
		if compare(val, p.topRows[len(p.topRows)-1].value) {
			p.topRows[len(p.topRows)-1] = TopRow{row: row, value: val}
			elementAdded = true
		}

	}
	if elementAdded {
		sort.Slice(p.topRows, func(i, j int) bool {
			return compare(p.topRows[i].value, p.topRows[j].value)
		})
	}

	return row, nil
}

// ProcessAggregation returns the top N rows.
func (p *TopNProcessor) ProcessAggregation([][]string) ([][]string, error) {
	result := make([][]string, len(p.topRows))
	for i, topRow := range p.topRows {
		result[i] = topRow.row
	}
	return result, nil
}

// compare function compares two string values. If they are numeric, it compares them as floats.
// Otherwise, it compares them as strings.
func compare(a, b string) bool {
	aFloat, aErr := strconv.ParseFloat(a, 64)
	bFloat, bErr := strconv.ParseFloat(b, 64)

	if aErr == nil && bErr == nil {
		return aFloat > bFloat
	}

	return a > b
}

// Ceil Processor returns the ceiling of a numeric cell
type CeilProcessor struct {
	BaseProcessor
}

func Ceil() Processor {
	return &CeilProcessor{}
}

func (p *CeilProcessor) ProcessRow(row []string, rowIndex int) ([]string, error) {
	result := make([]string, len(row))
	for i, val := range row {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			result[i] = strconv.Itoa(int(math.Ceil(f)))
		} else {
			return nil, fmt.Errorf("%T Error: row %d, column %d, failed to parse float from string '%s': %v", p, rowIndex, i, val, err)
		}
	}
	return result, nil
}

func (p *CeilProcessor) ProcessAggregation(table [][]string) ([][]string, error) {
	return aggregateRows(table, p.ProcessRow)
}

// Pipeline to connect and process all processors
type DataPipeline struct {
	BaseProcessor
	filePath   string
	processors Processor
}

// Used to construct the data pipeline
func Read(filePath string) *DataPipeline {
	return &DataPipeline{filePath: filePath}
}

// Chains processor to list of processors in DataPipeline
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
	//Open input file
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

	hasAggregator := false
	for proc := dp.processors; proc != nil; proc = proc.Next() {
		if proc.isAggregator() {
			hasAggregator = true
		}
	}

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

		for proc := dp.processors; proc != nil; proc = proc.Next() {
			currentRow, err = proc.ProcessRow(currentRow, rowIndex)
			if err != nil {
				return err
			}
			if currentRow == nil {
				break
			}
		}

		if !hasAggregator && len(currentRow) != 0 {
			if err := writer.Write(currentRow); err != nil {
				return fmt.Errorf("failed to write row %d: %v", rowIndex, err)
			}
		}

		rowIndex++
	}

	if hasAggregator {
		var currentTable [][]string
		for proc := dp.processors; proc != nil; proc = proc.Next() {
			currentTable, err = proc.ProcessAggregation(currentTable)
			if err != nil {
				return err
			}
		}
		for _, currentRow := range currentTable {
			if err := writer.Write(currentRow); err != nil {
				return fmt.Errorf("failed to write rows from in-memory table: %v", err)
			}
		}
	}
	return nil
}

//Main

func main() {
	//Simple Chain
	err := Read("main/input.csv").
		With(GetRows(1, 1000)).
		With(GetColumns(1, 3)).
		Write("output_1.csv")

	if err != nil {
		fmt.Println("Error for first chain:", err)
	}

	//Second chain
	err = Read("main/input.csv").
		With(GetRows(1, 100)).
		With(GetColumns(5, 50)).
		With(ForEveryColumn(func(cell string) string {
			n, err := strconv.Atoi(cell)
			if err != nil {
				return cell
			}
			return strconv.Itoa(n * 2)
		})).
		Write("output_2.csv")

	if err != nil {
		fmt.Println("Error for second chain:", err)
	}

	//Third chain
	err = Read("main/input.csv").
		With(GetRows(1, 500)).
		With(GetColumns(10, 11)).
		With(TopN(50, 0)).
		Write("output_3.csv")

	if err != nil {
		fmt.Println("Error for third chain:", err)
	}

	err = Read("main/input.csv").
		With(GetRows(1, 500)).
		With(GetColumns(10, 11)).
		With(GetAvg()).
		Write("output_4.csv")

	if err != nil {
		fmt.Println("Error for fourth chain:", err)
	}
}
