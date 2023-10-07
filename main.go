package main

import (
	"dframe_arrow_research/dataframe"
	"dframe_arrow_research/memory"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v12/arrow/memory"
)

type DummyAllocator struct {
	internalAlloc *arrow_memory.GoAllocator
}

func NewDummyAllocator() *DummyAllocator {
	return &DummyAllocator{internalAlloc: arrow_memory.NewGoAllocator()}
}

func (a *DummyAllocator) Allocate(size int) []byte {
	return a.Allocate(size)
}

func (a *DummyAllocator) Reallocate(size int, b []byte) []byte {
	return a.Reallocate(size, b)
}

func (a *DummyAllocator) Free(b []byte) {}

// To-Do, update types that need to adhere to interface to do so where it wont compile otherwise
func main() {
	dfResult := dataframe.ReadCSVFromFile("testdata/csvdata/baseint64.csv")
	if dfResult.GetErr() != nil {
		panic(dfResult.GetErr())
	}
	df := dfResult.Get()
	df.Print()
	defer df.Release()

	dealloc := memory.NewBaseDeallocator()

	sResult := df.Column("C1").Get().Add([]int64{1000, 222, 34567}, dealloc)
	if sResult.GetErr() != nil {
		panic(sResult.GetErr())
	}
	println(sResult.Get().String())
	err := df.AddColumn(sResult.Get().Add(df.Column("C2"), dealloc).Get().Rename("Dean").Get()) // Ownership Taken
	if err != nil {
		panic(err)
	}
	df.Print()
	defer dealloc.ReleaseResources() // Will Release Releasables(Non-Owned)

}

// To-Do, Could Remove Or Add To Examples
func ExploringArrow() {
	pool := arrow_memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	for i, col := range rec.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}
}

func ArrayStringInvestigation() {
	builder := array.NewStringBuilder(arrow_memory.DefaultAllocator)
	builder.AppendValues([]string{"happy", "saddddd", "not ready to see how the world works"}, nil)

	stringArray := builder.NewStringArray()
	fmt.Println(stringArray)
	fmt.Println(stringArray.Len())
}

func SeriesPotentialNullResearch() {

	// 4 Main Ways of Getting Values(There are others but main focus for now..)
	// These ways may have sub forms in terms of how implemented

	// Get Data -> Always of Type T
	// Get Detail -> Could Be of Type T or Null
	// Get [] Data -> Always of Type T

	// Grouped Data -> Could Be of Type T or Null
	// This Could Be in Two Forms:
	// - A Slice of structs, where each individual struct has a Type T or Null
	// - A Struct with two slices, the values and the bitmap on if null
}

func ArrayPrintingTest() {
	// Create an Int32 array
	builder := array.NewInt32Builder(arrow_memory.DefaultAllocator)
	builder.AppendValues([]int32{1, 2, 3, 4, 5}, []bool{true, false, true, false, true})
	int32Array := builder.NewArray()

	fmt.Println(int32Array)
}

func MutableFun() {
	// Create an Int32 array
	builder := array.NewInt32Builder(arrow_memory.DefaultAllocator)
	builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	int32Array := builder.NewInt32Array()
	_ = arrow.PrimitiveTypes.Date32

	// Get a copy of the internal slice as []int32(Not actually a copy)
	int32Slice := int32Array.Int32Values()

	// Modify the copied slice(Not a copy)
	int32Slice[0] = 11

	// Print the original array to show that it remains unchanged(It Does change!)
	fmt.Println(int32Slice)
	fmt.Println(int32Array.Int32Values())
}

func Float64Example() {
	pool := arrow_memory.NewGoAllocator()

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	b.AppendValues(
		[]float64{1, 2, 3, -1, 4, 5},
		[]bool{true, true, true, false, true, true},
	)

	deallocator := memory.NewBaseDeallocator()

	arr := b.NewFloat64Array()
	defer arr.Release()
	// deallocator.AddResourceToCleanup(arr)
	deallocator.AddResourceToCleanup(arr)

	fmt.Printf("array = %v\n", arr)

	sli := array.NewSlice(arr, 2, 5).(*array.Float64)
	defer sli.Release()
	// deallocator.AddResourceToCleanup(sli)

	fmt.Printf("slice = %v\n", sli)

	for _, _ = range arr.NullBitmapBytes() {
	}
}

func printarrow_memoryStats() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Printf("Allocated arrow_memory: %v bytes\n", memStats.Alloc)
	fmt.Printf("Total arrow_memory allocated and not yet freed: %v bytes\n", memStats.TotalAlloc)
	fmt.Printf("Heap arrow_memory allocated: %v bytes\n", memStats.HeapAlloc)
	fmt.Println("===================================================")
}

func TestRunTimearrow_memory() {
	printarrow_memoryStats()
	pool := arrow_memory.NewGoAllocator()
	// pool := NewDummyAllocator()

	arrow_memoryTestRange := 50
	deallocator := memory.NewBaseDeallocator()

	printarrow_memoryStats()
	for index := 0; index < arrow_memoryTestRange; index++ {
		b := array.NewFloat64Builder(pool)
		defer b.Release()

		deepBool := false
		if index%2 == 0 {
			deepBool = true
		}

		b.AppendValues(
			[]float64{float64(index), 2, 3, -1, float64(index) * 2, 5},
			[]bool{true, true, deepBool, false, true, true},
		)

		arr := b.NewArray()
		deallocator.AddResourceToCleanup(arr)
	}

	printarrow_memoryStats()
	deallocator.ReleaseResources()
	printarrow_memoryStats()

	// defer arr.Release()
	// deallocator.AddResourceToCleanup(arr)
	// deallocator.AddResourceToCleanup(&arr)
}

func TestParallelArray() {
	// Create an array builder
	builder := array.NewInt32Builder(arrow_memory.DefaultAllocator)

	// Add elements to the array builder
	builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	array := builder.NewArray().(*array.Int32)

	// Calculate the average in parallel using goroutines and vectorization
	// numCPU := runtime.NumCPU()
	// chunkSize := array.Len() / numCPU
	numCPU := 1
	chunkSize := array.Len() / numCPU

	var wg sync.WaitGroup
	wg.Add(numCPU)
	sum := int64(0)

	for i := 0; i < numCPU; i++ {
		println("In i")
		go func(start, end int) {
			println("In g")
			defer wg.Done()

			localSum := int64(0)
			for j := start; j < end; j++ {
				localSum += int64(array.Int32Values()[j])
				print("In j")
			}
			atomic.AddInt64(&sum, localSum)
		}(i*chunkSize, (i+1)*chunkSize)
	}

	wg.Wait()

	average := float64(sum) / float64(array.Len())

	fmt.Printf("Array: %v\n", array)
	fmt.Printf("Average: %f\n", average)
}
