package main

//
//import (
//	"encoding/csv"
//	"fmt"
//	"io"
//	"os"
//	"strconv"
//)
//
//type TableOperation interface {
//	Apply(table [][]string) [][]string
//}
//
//type TableProcessor struct {
//	Operations []TableOperation
//}
//
//func (tp *TableProcessor) Apply(table [][]string) [][]string {
//	for _, op := range tp.Operations {
//		table = op.Apply(table)
//	}
//	return table
//}
//
//func Read(filename string) *TableProcessor {
//	// Read operation
//	op := &ReadOperation{Filename: filename}
//	return &TableProcessor{Operations: []TableOperation{op}}
//}
//
//func (tp *TableProcessor) With(op TableOperation) *TableProcessor {
//	tp.Operations = append(tp.Operations, op)
//	return tp
//}
//
//func (tp *TableProcessor) Write(outputFilename string) error {
//	// Apply all operations
//	table := make([][]string, 0)
//	for _, op := range tp.Operations {
//		table = op.Apply(table)
//	}
//
//	// Write to output file
//	file, err := os.Create(outputFilename)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	writer := csv.NewWriter(file)
//	for _, record := range table {
//		if err := writer.Write(record); err != nil {
//			return err
//		}
//	}
//	writer.Flush()
//	return writer.Error()
//}
//
//type ReadOperation struct {
//	Filename string
//}
//
//func (op *ReadOperation) Apply(table [][]string) [][]string {
//	file, err := os.Open(op.Filename)
//	if err != nil {
//		fmt.Println("Error opening file:", err)
//		return table
//	}
//	defer file.Close()
//
//	reader := csv.NewReader(file)
//	for {
//		record, err := reader.Read()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			fmt.Println("Error reading CSV:", err)
//			return table
//		}
//		table = append(table, record)
//	}
//	return table
//}
//
//func example() {
//	err := Read("input.csv").
//		With(ForEveryColumn(func(cell string) string {
//			n, _ := strconv.Atoi(cell)
//			return strconv.Itoa(n * 2)
//		})).
//		With(GetColumns(3, 5)).
//		With(GetRows(7, 20)).
//		With(SumRow()).
//		Write("output.csv")
//
//	if err != nil {
//		fmt.Println("Error:", err)
//		return
//	}
//	fmt.Println("Operations completed successfully")
//}
//
//// Implementations
//func ForEveryColumn(fn func(string) string) TableOperation {
//	return &ColumnOperation{Fn: fn}
//}
//
//type ColumnOperation struct {
//	Fn func(string) string
//}
//
//func (op *ColumnOperation) Apply(table [][]string) [][]string {
//	for _, row := range table {
//		for i, cell := range row {
//			row[i] = op.Fn(cell)
//		}
//	}
//	return table
//}
//
//func GetColumns(indices ...int) TableOperation {
//	return &GetColumnsOperation{Indices: indices}
//}
//
//type GetColumnsOperation struct {
//	Indices []int
//}
//
//func (op *GetColumnsOperation) Apply(table [][]string) [][]string {
//	result := make([][]string, len(table))
//	for i, row := range table {
//		result[i] = make([]string, len(op.Indices))
//		for j, idx := range op.Indices {
//			result[i][j] = row[idx]
//		}
//	}
//	return result
//}
//
//func GetRows(start, end int) TableOperation {
//	return &GetRowsOperation{Start: start, End: end}
//}
//
//type GetRowsOperation struct {
//	Start, End int
//}
//
//func (op *GetRowsOperation) Apply(table [][]string) [][]string {
//	return table[op.Start:op.End]
//}
//
//func SumRow() TableOperation {
//	return &SumRowOperation{}
//}
//
//type SumRowOperation struct{}
//
//func (op *SumRowOperation) Apply(table [][]string) [][]string {
//	for i, row := range table {
//		sum := 0
//		for _, cell := range row {
//			n, _ := strconv.Atoi(cell)
//			sum += n
//		}
//		table[i] = append(row, strconv.Itoa(sum))
//	}
//	return table
//}
