package main

//
//type ChunkProcessor interface {
//	ProcessChunk(chunk [][]string) [][]string
//}
//
//type TableProcessor struct {
//	ChunkProcessor
//	Operations []TableOperation
//}
//
//func (tp *TableProcessor) Apply(table [][]string) [][]string {
//	chunkSize := 100 // Adjust the chunk size as needed
//	for i := 0; i < len(table); i += chunkSize {
//		end := i + chunkSize
//		if end > len(table) {
//			end = len(table)
//		}
//		chunk := table[i:end]
//
//		for _, op := range tp.Operations {
//			chunk = op.Apply(chunk)
//		}
//
//		table = tp.ProcessChunk(chunk)
//	}
//	return table
//}
//
//func Read(filename string) *TableProcessor {
//	// Read operation
//	op := &ReadOperation{Filename: filename}
//	return &TableProcessor{
//		ChunkProcessor: op,
//		Operations:     []TableOperation{op},
//	}
//}
