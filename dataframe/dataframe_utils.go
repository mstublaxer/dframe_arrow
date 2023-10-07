package dataframe

import (
	"dframe_arrow_research/types"
	"errors"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v12/arrow/memory"
	"golang.org/x/exp/constraints"
)

// Ensure Conformance to Interface
var _ types.SortableStruct = (*ComparisonResult)(nil)
var _ types.SortableStruct = (*UniqueRecord)(nil)

type ComparisonResult struct {
	Index   int
	IsValid bool
}

func (c ComparisonResult) Value() int {
	return c.Index
}

type UniqueRecord struct {
	Record arrow.Record
	Id     int
}

func (r UniqueRecord) Value() int {
	return r.Id
}

type RecordRange struct {
	Start int
	End   int
}

// Internal Type Instantiations For Base Priority Queue
type ComparisonResultPriorityQueue = types.BasePriorityQueue[ComparisonResult]
type UniqueRecordPriorityQueue = types.BasePriorityQueue[UniqueRecord]

func NewEmptyRecord() arrow.Record {
	pool := arrow_memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	rec := b.NewRecord()
	return rec
}

// private ...
func validateSeriesLength[T any](arr []T, n int64) error {
	seriesLength := int64(len(arr))

	// Uninitialized Record Will Always Take the incoming data
	// So Always valid
	if n == 0 {
		return nil
	}

	if seriesLength != n {
		return errors.New("Invalid Series Length")
	}

	return nil
}

// To-Do, describe better
func getRecordRanges(validRows []bool) []RecordRange {
	ranges := make([]RecordRange, 0)

	validDataFound := false
	index := 0

	for i, isValid := range validRows {
		if isValid {
			index = i
			validDataFound = true
			break
		}
	}

	currentRecordRange := RecordRange{Start: index}
	currResult := ComparisonResult{Index: index, IsValid: validDataFound}
	prevResult := currResult

	for i := index + 1; i < len(validRows); i++ {
		currResult = ComparisonResult{Index: i, IsValid: validRows[i]}

		// New Range
		if currResult.IsValid && !prevResult.IsValid {
			currentRecordRange.Start = currResult.Index
		}

		// End Of a Range Met
		if !currResult.IsValid && prevResult.IsValid {
			currentRecordRange.End = currResult.Index - 1
			ranges = append(ranges, currentRecordRange)
		}

		prevResult = currResult
	}

	if currResult.IsValid {
		currentRecordRange.End = currResult.Index
		ranges = append(ranges, currentRecordRange)
	}

	return ranges
}

// To Do, Some Type of Wrapper for the comparison only
func compare[T constraints.Ordered](index int, val T, target T, isNull bool, comparator Comparator, wg *sync.WaitGroup, ch chan<- ComparisonResult) {
	defer wg.Done()

	var result bool = false
	switch comparator {
	case CompFunc:
		panic("Functions Not Yet Supported!")
	case Eq:
		result = val == target
	case Neq:
		result = val != target
	case Gt:
		result = val > target
	case GtEq:
		result = val >= target
	case Less:
		result = val < target
	case LessEq:
		result = val <= target
	default:
		panic("Unsupported Comparator!")
	}

	// Send the result along with the index to the channel
	ch <- ComparisonResult{Index: index, IsValid: result}
}

func pullRecordSlice(index int, d *DataFrame, r *RecordRange, wg *sync.WaitGroup, ch chan<- UniqueRecord) {
	defer wg.Done()

	// To-Do, need to understand new slice i:j better
	newRecord := d.record.NewSlice(int64(r.Start), int64(r.End)+1)
	ch <- UniqueRecord{Record: newRecord, Id: index}
}
