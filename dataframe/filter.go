package dataframe

import (
	"container/heap"
	"fmt"
	"runtime"
	"sync"
)

type LogicalOp string

// Supported OPs
const (
	OpNone LogicalOp = "NONE"
	OpAnd  LogicalOp = "AND"
	OpOr   LogicalOp = "OR"
)

type Comparator string

// Valid Comparators
const (
	CompFunc Comparator = "func" // Custom Compare Func
	Eq       Comparator = "=="   // Equal
	Neq      Comparator = "!="   // Not equal
	Gt       Comparator = ">"    // Greater than
	GtEq     Comparator = ">="   // Greater than or Equal to
	Less     Comparator = "<"    // Less than
	LessEq   Comparator = "<="   // Less than or Equal To
)

// To-Do, unused until function support
type IgnoreArg struct{}

// Same example as above but with a custom function:
//
//	Filter{Comparator: func(f float64) bool { return f > 1.2 }, Column: "COL1"}
type Filter struct {
	column     string
	comparator Comparator // <- this should be constrained so that it can only be a function filter or a Comparison filter
	arg        any
	logicalOp  LogicalOp
	filters    []*Filter
}

// Example of Built in Operators
// F("Column1", ">", 5).And(F("Column2", "<", 5))
//
// TO-DO, implement custom funcs
// Example using Custom Functions
// customFunc := func(f float64) bool { return f > 1.2 }
// F("Column1", customFunc, IgnoreArg).And(F("Column2", "<", 5))

// Creates a New Filter Object
func F(column string, comparator Comparator, arg any) *Filter {
	return &Filter{column, comparator, arg, OpNone, make([]*Filter, 0)}
}

func (self *Filter) appendedSelf(f *Filter, op LogicalOp) *Filter {
	self.filters = append(self.filters, f)

	// Track The Operation For Left -> Right Execution
	// e.g. - Filter1.And(Filter2)
	// Filter1 checks Filter2, Operation = AND
	// Filter1 AND Filter2
	f.logicalOp = op
	return self
}

func (self *Filter) And(f *Filter) *Filter {
	return self.appendedSelf(f, OpAnd)
}

func (self *Filter) Or(f *Filter) *Filter {
	return self.appendedSelf(f, OpOr)
}

// To-Do Implement
func (self Filter) String() string {
	return "Unimplemented"
}

// Recursive Method That given a DataFrame can go down the Filter Chain
// Get a Bool Slice for every row in every chosen column and perform the operations
// from left->right at each level
func (f *Filter) applyFilter(d *DataFrame) []bool {

	// To-Do, Consider a Traversal For Error Checking First Maybe as validator?
	// To-Do, consider returning an error here? Or should we be panicking?

	var boolSlices [][]bool = make([][]bool, len(f.filters)+1)

	// Add Self First
	selfSlice := f.getColumnResults(d)
	boolSlices[0] = selfSlice

	// Get Right Slices
	for index, filter := range f.filters {
		boolSlice := filter.applyFilter(d)
		boolSlices[index+1] = boolSlice
		println(filter.column)
	}

	// Resolve boolSlices from left -> right
	// To-do, could use goroutines and channels here but there's probably some overhead at small sizes
	// maybe consider some type of tests across devices for benchmarking ecosystems
	// then use a size check that generates the bool size based on an inferrence of which method is more
	// efficient?
	// Or do we not care about overhead for small cases because as we get reasonably larger theres some payoff?
	// Excess Premature optimization also has its downsides, might be worth just revisiting

	// Temp For Debugging
	result := make([]bool, len(boolSlices[0]))
	copy(result, boolSlices[0])

	for i := 1; i < len(boolSlices); i++ {
		var wg sync.WaitGroup

		resultChan := make(chan struct {
			index int
			value bool
		}, len(result))

		currentLogicalOp := f.filters[i-1].logicalOp

		// to-do, please refactor lol, shouldnt be doing the loops like this
		comparisonSlice := boolSlices[i]
		switch currentLogicalOp {
		case OpAnd:
			for j := 0; j < len(result); j++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					resultChan <- struct {
						index int
						value bool
					}{index, result[index] && comparisonSlice[index]}
				}(j)
			}
		case OpOr:
			for j := 0; j < len(result); j++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					resultChan <- struct {
						index int
						value bool
					}{index, result[index] || comparisonSlice[index]}
				}(j)
			}
		default:
			panic("Should Not Occur during bool slice compare")
		}

		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect Results
		for res := range resultChan {
			result[res.index] = res.value
		}
	}

	return result
}

// Get The Actual Results in a Column across all of its rows for a filter
func (f *Filter) getColumnResults(d *DataFrame) []bool {
	pQueue := &ComparisonResultPriorityQueue{}
	heap.Init(pQueue)

	numCPU := runtime.NumCPU()
	chunkSize := int(d.record.NumRows()) / numCPU
	chunkSize = max(chunkSize, 1)

	var wg sync.WaitGroup
	resultCh := make(chan ComparisonResult, d.record.NumRows()) // Use a custom struct to hold index and result

	seriesResult := d.ColumnC(f.column, false)
	if seriesResult.GetErr() != nil {
		panic(fmt.Sprintf("Invalid/NonExistent Column %v", f.column))
	}

	column := seriesResult.Get()
	anyElements, err := column.Elements()
	if err != nil {
		panic("[getColumnResults] Issue Retrieving Column Elements")
	}

	// To-Do, Support Other Types
	switch typedValues := anyElements.Values.(type) {
	case []int32:

		// To-Do, Wrap This
		// Start goroutines to send data to the channel
		for i := 0; i < int(d.record.NumRows()); i++ {
			wg.Add(1)

			// To-Do Handle Error and hardcoding
			// To-Do, this really should handle any downstream type supported
			// int8, int16, int32, int
			ok := true
			var value int32
			switch v := f.arg.(type) {
			case int8:
				value = int32(v)
			case int16:
				value = int32(v)
			case int32:
				value = int32(v)
			case int:
				value = int32(v)
			default:
				ok = false
			}
			if !ok {
				panic("Wrong Arg For Type")
			}
			go compare(i, typedValues[i], value, anyElements.IsNullValues[i], f.comparator, &wg, resultCh)
		}

	default:
		panic("Unsupported Type!")
	}

	// Close the data channel after all data is sent
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	resultSlice := make([]bool, d.record.NumRows())
	for res := range resultCh {
		heap.Push(pQueue, res)
	}

	index := 0
	for pQueue.Len() > 0 {

		// To-Do, possible cast failure
		resultSlice[index] = heap.Pop(pQueue).(ComparisonResult).IsValid
		index++
	}

	return resultSlice
}
