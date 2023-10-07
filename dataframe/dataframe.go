package dataframe

import (
	"container/heap"
	"dframe_arrow_research/memory"
	"dframe_arrow_research/types"
	"dframe_arrow_research/utils"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/csv"
)

type ArrowRecordResult = types.Result[arrow.Record]
type DataFrameResult = types.Result[*DataFrame]

func newDataFrameResult(s *DataFrame, err error) DataFrameResult {
	return types.NewResult(s, err)
}

func newArrowRecordResult(r arrow.Record, err error) ArrowRecordResult {
	return types.NewResult(r, err)
}

// DataFrame structure
// Internally Leverages arrow.Record structure from golang apache arrow while also
// adding custom functionality via a Series Array
type DataFrame struct {
	record arrow.Record
	arr    []Series
}

// Add a Settings Object so that things like allocators etc can be configured?
// Return a Dataframe result?
func New() DataFrame {

	// To-Do, Can Specify Allocator?

	return DataFrame{
		record: NewEmptyRecord(),
		arr:    make([]Series, 0),
	}
}

// To-Do, Implement
// To-Do, Wrappers For Low Level Control?
func ReadCSVFromFile(path string) DataFrameResult {

	// To-Do, No Need To Code This to 20? Handle Differently?
	recordCh := make(chan ArrowRecordResult, 20)

	go func() {
		// Close When Done
		defer close(recordCh)
		file, err := os.Open(path)
		if err != nil {
			// Pass Off Failure To Be Handled
			recordCh <- types.NewResult[arrow.Record](nil, err)
			file.Close()
			return
		}

		defer file.Close()

		// Will Currently Always Infer a Hedaer
		// To-Do, I like this
		rdr := csv.NewInferringReader(
			file,
			csv.WithChunk(5000),
			csv.WithHeader(true),
			csv.WithNullReader(true, "[]", "null", "NULL", ""),
		)

		for rdr.Next() {
			if rdr.Err() != nil {
				recordCh <- types.NewResult[arrow.Record](nil, rdr.Err())
				return
			}

			rec := rdr.Record()
			rec.Retain()
			recordCh <- newArrowRecordResult(rec, nil)
		}

	}()

	records := make([]arrow.Record, 0)

	// To-Do, When Inferring NonSupported Types, Converted to Supported Types
	for result := range recordCh {
		if result.GetErr() != nil {
			// To-Do, determine types of errors, some seem recoverable like
			// reading in a multitype situation, maybe the answer is to convert to a string column etc
			return newDataFrameResult(nil, result.GetErr())
		}

		records = append(records, result.Get())
	}

	newDf := NewFromRecords(records)
	return newDf
}

func (d *DataFrame) AddColumn(s Series) error {
	return d.AddSeries(s)
}

// To-Do, Could Add Utility Functions For Head and Tail that wrap around them with default values

func (d *DataFrame) Head(numRows int, dealloc *memory.Deallocator) DataFrameResult {
	actualNumRows := min(int64(numRows), d.record.NumRows())
	return d.getCut(0, actualNumRows)
}

func (d *DataFrame) Tail(numRows int, dealloc *memory.Deallocator) DataFrameResult {
	actualNumRows := min(int64(numRows), d.record.NumRows())
	return d.getCut(d.record.NumRows()-actualNumRows, d.record.NumRows())
}

// To-Do, Deallocator here?
// To-Do, make pub?
func (d *DataFrame) getCut(startRow int64, endRow int64) DataFrameResult {

	if startRow > d.record.NumRows() || endRow > d.record.NumRows() {
		return newDataFrameResult(nil, errors.New("Cannot Get Cut of DataFrame beyond rows!"))
	}

	if startRow < 0 || endRow < 0 {
		return newDataFrameResult(nil, errors.New("Row Must Be >= 0 For Valid Operation!"))
	}

	// Create an array of Series the length of columns
	newSeriesArr := make([]Series, d.record.NumCols())
	// columns := make([]arrow.Array, d.record.NumCols())

	// Go Through All Columns In Record
	// Create in place new slice
	// Create a series from each arrow.Array, place in array
	for i := 0; i < int(d.record.NumCols()); i++ {

		newColumn := array.NewSlice(d.record.Column(i), startRow, endRow)
		// columns[i] = newColumn

		seriesResult := nonReleasableFromArrow(d.arr[i].Name(), newColumn)
		if seriesResult.GetErr() != nil {
			return newDataFrameResult(nil, seriesResult.GetErr())
		}
		newSeriesArr[i] = seriesResult.Get()
	}

	// Make a Record
	// Make DataFrame
	newDataFrame := &DataFrame{
		record: d.record.NewSlice(startRow, endRow),
		arr:    newSeriesArr,
	}

	// To-Do, Add The Above DataFrame to deallocator
	return newDataFrameResult(newDataFrame, nil)
}

// Function is applied to each series in the DataFrame
// Each call will generate a new series
// A New DataFrame from all Series will be created
func (d *DataFrame) ColumnApply(fn func(Series) SeriesResult) DataFrame {
	panic("Unimplemented")
}

// Take ownership of a Series and add it to the DataFrame
// Series added to a DataFrame become non-releasable as
// the DataFrame takes ownership and will release them on DataFrame release
func (d *DataFrame) AddSeries(series Series) error {

	// We don't allow new names(could add a schema for overwriting instead)
	err := d.validateSeriesName(series.Name())
	if err != nil {
		return err
	}

	// Handle Mismatched Length Case
	if d.record.NumCols() > 0 {
		if series.Len() != int(d.record.NumRows()) {
			return errors.New(fmt.Sprintf("Invalid Series Length %v, Expected: %v", series.Len(), d.record.NumRows()))
		}
	}

	// Setup For an updated record
	newColumnLength := d.record.NumCols() + 1
	fieldList := make([]arrow.Field, newColumnLength)
	columns := make([]arrow.Array, newColumnLength)

	// Copy Over Existing Data
	copy(fieldList, d.record.Schema().Fields())
	copy(columns, d.record.Columns())

	// Add Information on new field to be added
	// To-Do, Maybe some static map can be used for mapping arrow primitives
	fieldList[d.record.NumCols()] = arrow.Field{
		Name: series.Name(),
		Type: series.Type(),
	}

	// Get the underlying arrow array and add it to for the updated record
	newArray := series.getArray()
	columns[d.record.NumCols()] = newArray
	meta := d.record.Schema().Metadata()
	newSchema := arrow.NewSchema(fieldList, &meta)
	newRecord := array.NewRecord(newSchema, columns, int64(newArray.Len()))
	d.record.Release()   // <- Created New Record, Release Current
	d.record = newRecord // <- Update Record

	// If We Get This Far, Take Ownership of the Series
	// Only DataFrame can release this series now
	series.setReleasable(false)
	d.arr = append(d.arr, series)
	return nil
}

func (d *DataFrame) validateSeriesName(column string) error {
	if column == "" {
		return errors.New("Series/Field name cannot be empty string")
	}

	for _, field := range d.record.Schema().Fields() {
		if column == field.Name {
			return errors.New("Series/Field names cannot be identical")
		}
	}

	return nil
}

// to-do, should be returning a DataFrame result
// to-do, can I wrap any of the goroutine stuff in common logic anywhere in utils?
// should that always be more explicit ?
func (d *DataFrame) Filter(f *Filter) DataFrameResult {

	finalSlice := f.applyFilter(d)

	ranges := getRecordRanges(finalSlice)

	for _, val := range ranges {
		println(fmt.Sprintf("%v %v", val.Start, val.End))
	}

	pq := &UniqueRecordPriorityQueue{}
	heap.Init(pq)

	// Create a set of parallelism and go off and create a whole bunch of sliced records using getCut
	numCPU := runtime.NumCPU()
	chunkSize := int(d.record.NumRows()) / numCPU
	chunkSize = max(chunkSize, 1)

	var wg sync.WaitGroup

	resultCh := make(chan UniqueRecord, len(ranges))

	// Start goroutines to send data to the channel
	for i := 0; i < len(ranges); i++ {
		wg.Add(1)
		go pullRecordSlice(i, d, &ranges[i], &wg, resultCh)
	}

	// Close the data channel after all data is sent
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		heap.Push(pq, res)
	}

	records := make([]arrow.Record, len(ranges))

	index := 0
	for pq.Len() > 0 {
		records[index] = heap.Pop(pq).(UniqueRecord).Record
		index++
	}

	newDf := NewFromRecords(records)
	return newDf
}

func NewFromRecords(records []arrow.Record) DataFrameResult {

	if len(records) == 0 {
		defaultDf := New()
		return newDataFrameResult(&defaultDf, nil)
	}

	initialRecord := records[0]
	columns := make([]arrow.Array, initialRecord.NumCols())
	seriesArr := make([]Series, initialRecord.NumCols())

	for i := 0; i < int(initialRecord.NumCols()); i++ {

		var newArray arrow.Array

		datatype := initialRecord.Column(i).DataType()
		switch datatype {
		case arrow.PrimitiveTypes.Int8:
			{
				int8Slice, err := utils.CreateSliceFromRecords[int8](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				int8Arr, err := utils.NewArrowArray(int8Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = int8Arr
			}
		case arrow.PrimitiveTypes.Int16:
			{
				int16Slice, err := utils.CreateSliceFromRecords[int16](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				int16Arr, err := utils.NewArrowArray(int16Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = int16Arr
			}
		case arrow.PrimitiveTypes.Int32:
			{
				int32Slice, err := utils.CreateSliceFromRecords[int32](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				int32Arr, err := utils.NewArrowArray(int32Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = int32Arr
			}
		case arrow.PrimitiveTypes.Int64:
			{
				int64Slice, err := utils.CreateSliceFromRecords[int64](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				int64Arr, err := utils.NewArrowArray(int64Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = int64Arr
			}
		case arrow.PrimitiveTypes.Float32:
			{
				float32Slice, err := utils.CreateSliceFromRecords[float32](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				float32Arr, err := utils.NewArrowArray(float32Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = float32Arr
			}
		case arrow.PrimitiveTypes.Float64:
			{
				float64Slice, err := utils.CreateSliceFromRecords[float64](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				float64Arr, err := utils.NewArrowArray(float64Slice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = float64Arr
			}
		case arrow.BinaryTypes.String:
			{
				stringSlice, err := utils.CreateSliceFromRecords[string](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				stringArr, err := utils.NewArrowArray(stringSlice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = stringArr
			}
		case arrow.FixedWidthTypes.Boolean:
			{
				boolSlice, err := utils.CreateSliceFromRecords[bool](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				boolArr, err := utils.NewArrowArray(boolSlice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = boolArr
			}
		case arrow.PrimitiveTypes.Date32:
			{
				dateSlice, err := utils.CreateSliceFromRecords[arrow.Date32](records, i)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				dateArr, err := utils.NewArrowArray(dateSlice)
				if err != nil {
					return newDataFrameResult(nil, err)
				}

				newArray = dateArr
			}

		default:
			panic("Unsupported Type Being Used in Record")
		}

		columns[i] = newArray

		// create series
		seriesResult := nonReleasableFromArrow(initialRecord.ColumnName(i), columns[i])
		if seriesResult.GetErr() != nil {
			panic("to-do handle dframe!") // <- To-Do handle better
		}

		seriesArr[i] = seriesResult.Get()
	}

	meta := initialRecord.Schema().Metadata()
	newSchema := arrow.NewSchema(initialRecord.Schema().Fields(), &meta)
	newRecord := array.NewRecord(newSchema, columns, int64(columns[0].Len()))

	newDf := DataFrame{
		record: newRecord,
		arr:    seriesArr,
	}

	return newDataFrameResult(&newDf, nil)
}

func (d *DataFrame) Shape() (int64, int64) {
	return d.record.NumRows(), d.record.NumCols()
}

func (d *DataFrame) String() {
	panic("Unimplemented")
}

func (d *DataFrame) PrintColumnNames() {
	for i := range d.record.Columns() {
		fmt.Printf("column[%d] %q\n", i, d.record.ColumnName(i))
	}
}

// To-Do, Seriously Needs to Improve
func (d *DataFrame) Print() {
	for i, col := range d.record.Columns() {
		if col.DataType() == arrow.PrimitiveTypes.Date32 {
			dates, ok := col.(*array.Date32)
			if ok {
				var datesAsStr string = " "
				for _, arrowDate := range dates.Date32Values() {
					datesAsStr += arrowDate.FormattedString() + " "
				}
				println(fmt.Sprintf("column[%d] %q: [%v]", i, d.record.ColumnName(i), datesAsStr))
			}
		} else {
			println(fmt.Sprintf("column[%d] %q: %v", i, d.record.ColumnName(i), col))
		}
	}
}

// Return Copy
func (d *DataFrame) Column(name string) SeriesResult {
	return d.ColumnC(name, true)
}

// To-Do Describe better
func (d *DataFrame) ColumnC(name string, copy bool) SeriesResult {

	// If Copy, Return a Copy with releasable set to true, user has to handle themself
	// If Not Copy, User Can Only Use The Returned Series as long as associated
	// DataFrame hasn't been released, otherwise undefined behavior can occur
	// What happens if user sets interface to different value?

	// -> Find Column By Name

	// -> Did anything go wrong?
	/*_, ok := d.record.Schema().FieldsByName(name)
	if !ok {
		return newSeriesResult(nil, errors.New("Column Does Not Exist!"))
	}
	*/
	if d.record.NumCols() <= 0 {
		return newSeriesResult(nil, errors.New("No Valid Columns Yet!"))
	}

	foundSeries := false
	var seriesIndex int64 = 0
	var i int64
	for i = 0; i < d.record.NumCols(); i++ {
		if d.record.ColumnName(int(i)) == name {
			seriesIndex = i
			foundSeries = true
			break
		}
	}

	// -> Create "Non-Releasable" Series on arrow.Array?
	// -> Make Series Result and Return

	if !foundSeries {
		return newSeriesResult(nil, errors.New("Error Finding Series in DataFrame!"))
	}

	series := d.arr[seriesIndex]

	if copy {
		seriesResult := d.arr[seriesIndex].Copy()
		if seriesResult.GetErr() != nil {
			return newSeriesResult(nil, seriesResult.GetErr())
		}
		series = seriesResult.Get()
	}
	return newSeriesResult(series, nil)
}

func (d *DataFrame) Release() {
	d.record.Release()

	// Series Cleanup
	for _, s := range d.arr {
		s.setReleasable(true)
		s.Release()
	}
}
