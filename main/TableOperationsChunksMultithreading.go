package main

//
//type TableProcessor struct {
//	ChunkProcessor
//	Operations []TableOperation
//}
//
//func (tp *TableProcessor) Apply(table [][]string) [][]string {
//	chunkSize := 100 // Adjust the chunk size as needed
//	numChunks := (len(table) + chunkSize - 1) / chunkSize
//	chunkResults := make(chan struct {
//		index int
//		chunk [][]string
//	}, numChunks)
//	chunkOrder := make(chan int, numChunks)
//
//	for i := 0; i < len(table); i += chunkSize {
//		end := i + chunkSize
//		if end > len(table) {
//			end = len(table)
//		}
//		chunk := table[i:end]
//
//		go func(index int, chunk [][]string) {
//			for _, op := range tp.Operations {
//				chunk = op.Apply(chunk)
//			}
//			chunkResults <- struct {
//				index int
//				chunk [][]string
//			}{index, chunk}
//			chunkOrder <- index
//		}(i, chunk)
//	}
//
//	// Collect results from goroutines
//	results := make(map[int][][]string)
//	for i := 0; i < numChunks; i++ {
//		index := <-chunkOrder
//		result := <-chunkResults
//		results[result.index] = result.chunk
//	}
//
//	// Aggregate chunks in order
//	var sortedChunks [][]string
//	for i := 0; i < len(table); i += chunkSize {
//		if result, ok := results[i]; ok {
//			sortedChunks = append(sortedChunks, result...)
//		}
//	}
//
//	return tp.ProcessChunk(sortedChunks)
//}
