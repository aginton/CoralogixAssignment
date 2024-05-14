
## About
This pipeline includes the functions given in the instructions, as well as `TopN()`, which returns the rows with the top n values for some column.
`TopN()` as well as `Avg()` are aggregator functions, which use as input and output a 2d table. This works because we don't have to keep all the data in memory; For `Avg()` we essentially have 1 row, and for `TopN()` we have n rows.



## Testing
Simple tests can be run using `simple_test.go`.



## Processor Interface

A `Processor` is an interface:

```go
type Processor interface {
   ProcessRow(row []string, rowIndex int) ([]string, error)
   With(Processor) Processor
   Next() Processor
   ProcessAggregation(table [][]string) ([][]string, error)
   isAggregator() bool
}
```



- Each processor has a method `ProcessRow()`, that has as input and output a single row of data.
- `ProcessAggregation()` is a method that has as input and output a table.
- Each processor may link to another processor. 
- A processor may or may not be an aggregator processor. Specific processors that are aggregators have fields in their struct that represent the state of the entire table at some point. These fields are updated during the `ProcessRow()` phase. For example, `AvgProcessor()` has fields representing the running sum and count for specific columns:

```go 
type AvgProcessor struct {
   BaseProcessor
   columnIndices []int
   sums          []int
   counts        []int
}
```
- `ProcessAggregation()` is only called if the pipeline contains at least one aggregator processor



## Data Processing
We iterate through the input file line by line, passing the input to `ProcessRow()` of the first processor. The output of `ProcessRow()` is then fed to the second processor, and so on. 

In the case that we don’t have any aggregator processors, then after a line has gone through all the processors, we can write the corresponding output directly to our output file. If the output is nil, then we of course don’t write anything.


## Aggregator Processors
In the case that we do have an aggregator function, then we still process each line using all the processors, but we don’t write as we iterate over each row. 
Instead, we must also call `ProcessAggregation()` for the chain of processors. 

The initial input for `ProcessAggregation()` is an empty table. For non-aggregator processors, `ProcessAggregation()` simply calls `ProcessRow()` on every row of its input table. 
Such processors will therefore return nil if they are fed nil. 

However, we know the Aggregation processors contain in memory some struct representing the state of our table (or that is used to build up the table). 
For example, `TopN()` contains the top N rows based on some column. When we call `ProcessAggregation()` on these aggregators, we therefore end up building up our entire table. 
This output can then be passed to other processors, e.g., `…With(TopN(5,10)).With(GetColumns(2,4)).Write("output.csv")`, or it can be written directly to output.


## Memory Considerations
Note this is making the assumption that we can fit in memory all the data needed by the aggregator functions to build up our table, and that after calling `ProcessAggregation()`, we end up with a small table. 

For `GetAvg()`, this is reasonable as we only store a few rows of data in memory (and we assume an entire row can fit into memory), and we end up outputting a single row. 

For `TopN()`, this is reasonable so long as the value of n is not very large. If n was very large, or if we had to implement sorting on the entire dataset, then we could split the data into chunks and save these to temporary files. Then we could use a k-way merge sort to merge the sorted files back into a single sorted output.


## Error Handling
The processors are implemented mainly so that they return an error if they are fed invalid data. For example, if `GetAvg()` gets a non-numeric cell, it throws an error. 

The only exception is the end indices for `GetRow()` and `GetColumns()` can be outside the length of the actual data. My reasoning was that a user may not know how many rows or columns he has in total. If you pass in more rows or more columns than actually exist, the processors will still only work on the actual data.

## Optimization
One minor optimization that I could have gone for is in the implementation of `TopN()`, I could have used a max-heap to store the top n rows, rather than sorting each time a new row is added. Since `TopN()` wasn’t included in the example functions, however, I decided to leave it.