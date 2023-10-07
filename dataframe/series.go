package dataframe

import (
	"dframe_arrow_research/memory"
	"dframe_arrow_research/types"
	"dframe_arrow_research/utils"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v12/arrow/memory"
)

// Ensure Conformance to Interface
var _ Series = (*basicSeries)(nil)

// 'Immutable' DataFrame Building Block
// Ways to Access Data
// IsNull(i int64)
// NullN() int
//
// To-Do Needs a Release Method?
// To-Do, Add a Method that Can coerce any column to a float64? (Non Supported Values Become NaNs?)
type Series interface {

	// Common Series Processing
	SeriesProcessor

	// Scalar Operations
	MathOperator

	// Handle Apply Functions across Series
	SeriesFnHandler

	// Memory Cleanup Purposes
	Releaser

	// Common Math Operation Support
	types.StatsCalculator

	// String Representation
	fmt.Stringer

	Name() string
	Type() arrow.DataType // <- Only Supporting Primitive Types Initially
	Sort() SeriesResult
	Subset(ids []uint32) SeriesResult
	Rename(name string) SeriesResult
	Len() int

	Element(i int) (SeriesElement, error)
	Elements() (SeriesElements, error)

	Copy() SeriesResult

	getArray() arrow.Array
}

type Releaser interface {
	Release() error
	IsReleasable() bool
	setReleasable(isReleasable bool)
}

// / General Processing with Series interacting for Series Results
type SeriesProcessor interface {
	// To-Do should be able to Sort By Column with a given ruleset or even a custom function
	Append(s *Series) SeriesResult // <- Pass By Pointer and only copy right when needed
}

// Operations for Applying a Value to a Series, Returning a result and Ensuring associated memory is handled
// If a User chooses not to provide a deallocator, they have to call Release() on the Series being used themselves,
// unless they Add the Series to a DataFrame which effectively transfers off that ownership, calling release
type MathOperator interface {
	Mul(value any, dealloc memory.Deallocator) SeriesResult
	Add(value any, dealloc memory.Deallocator) SeriesResult
	Sub(value any, dealloc memory.Deallocator) SeriesResult
	Div(value any, dealloc memory.Deallocator) SeriesResult
}

type SeriesFnHandler interface {
	ApplyFn(fn interface{}) SeriesResult     // <- Apply a Function On a Series, Return a new series
	ApplySeries(fn interface{}) SeriesResult // <- Apply a Function across two compatible typings/lengths , Return a new series

	ApplyFnByIds(fn interface{}, ids []uint32) SeriesResult                // <- Apply a Function On a Series, Return a new series
	ApplySeriesByIds(fn interface{}, s *Series, ids []uint32) SeriesResult // <- Apply a Function across two compatible typings/lengths , Return a new series
}

type SeriesResult = types.Result[Series]

func newSeriesResult(s Series, err error) SeriesResult {
	return types.NewResult[Series](s, err)
}

type SeriesElement = types.Element[any]

func newSeriesElement(val any, null bool) SeriesElement {
	return SeriesElement{
		Val:  val,
		Null: null,
	}
}

type SeriesElements = types.Elements

func newSeriesElements(values any, nullValues []bool) SeriesElements {
	return SeriesElements{
		Values:       values,
		IsNullValues: nullValues,
	}
}

type basicSeries struct {
	field      arrow.Field // <- Access Metadata?
	array      arrow.Array
	releasable bool
}

// Returns a Releasable Series That the user HAS to call Release() defer.Release() or use a deallocator.
// The Only Exception to this is setting a Series to a column, where it will be converted to non-releasable from then.
// Essentially passing ownership permanently to the respective DataFrame,
// Adding a Series to a DataFrame via a slice or arrow.Array is recommended vs using this method
// Should Likely be used sparingly but can essentially be used to take a supported slice or array arrow
// and provide Series based functionality on top of that
// TO-DO, should this work via generics instead of interfaces? Could then
// constrain types inherently?
func NewReleasableFromSlice(name string, data any) SeriesResult {

	var err error

	// Must Provide a Slice
	if data == nil {
		return newSeriesResult(nil, errors.New("No Slice Data Provided For Data Structure!"))
	}

	// Explicitly Releasable and should be handled somewhere(as opposed to automatic cleanup)
	newSeries := basicSeries{
		releasable: true,
	}

	newArray, err := utils.NewArrowArray(data)
	if err != nil {
		return newSeriesResult(nil, err)
	}

	newSeries.array = newArray
	newSeries.field = arrow.Field{
		Name: name,
		Type: newArray.DataType(),
	}

	return newSeriesResult(&newSeries, err)
}

// Retains on an Arrow Array, like the releasable above, the user HAS to call Release() defer.Release() or use a deallocator.
// The Only Exception to this is setting a Series to a column, where it will be converted to non-releasable from then.
// Essentially passing ownership permanently to the respective DataFrame,
// Adding a Series to a DataFrame via a slice or arrow.Array is recommended vs using this method
// TO-DO, should this work via generics instead of interfaces? Could then
// constrain types inherently?
func NewReleasableFromArrow(name string, arr arrow.Array) SeriesResult {
	if arr == nil {
		return newSeriesResult(nil, errors.New("No Array Provided For Data Structure!"))
	}

	// Explicitly Releasable and should be handled somewhere(as opposed to automatic cleanup)
	newSeries := basicSeries{
		releasable: true,
		array:      arr,
		field: arrow.Field{
			Name: name,
			Type: arr.DataType(),
		},
	}

	// To-do Validate DataType, only allow allowed/supported types

	return newSeriesResult(&newSeries, nil)
}

func nonReleasableFromArrow(name string, arr arrow.Array) SeriesResult {
	if arr == nil {
		return newSeriesResult(nil, errors.New("No Array Provided For Data Structure!"))
	}

	// Explicitly Releasable and should be handled somewhere(as opposed to automatic cleanup)
	newSeries := basicSeries{
		releasable: false,
		array:      arr,
		field: arrow.Field{
			Name: name,
			Type: arr.DataType(),
		},
	}

	// To-do Validate DataType, only allow allowed/supported types

	return newSeriesResult(&newSeries, nil)
}

func (s *basicSeries) Element(i int) (SeriesElement, error) {
	switch s.field.Type.ID() {
	case arrow.INT8:
		arr, err := castToTypedArrowArray[*array.Int8](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.INT16:
		arr, err := castToTypedArrowArray[*array.Int16](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.INT32:
		arr, err := castToTypedArrowArray[*array.Int32](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.INT64:
		arr, err := castToTypedArrowArray[*array.Int64](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.STRING:
		arr, err := castToTypedArrowArray[*array.String](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.FLOAT32:
		arr, err := castToTypedArrowArray[*array.Float32](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.FLOAT64:
		arr, err := castToTypedArrowArray[*array.Float64](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	case arrow.BOOL:
		arr, err := castToTypedArrowArray[*array.Boolean](*s)
		if err != nil {
			return SeriesElement{}, err
		}

		return newSeriesElement(arr.Value(i), arr.IsNull(i)), nil
	default:
		return SeriesElement{}, errors.New("Field misconfiguration error on Series")
	}
}

func (s *basicSeries) Elements() (SeriesElements, error) {

	// To-Do, could move to helper function
	nullValueArr := make([]bool, s.array.Len())
	for index := 0; index < s.array.Len(); index++ {
		nullValueArr[index] = s.array.IsNull(index)
	}

	// Check For Supported Arrow Types, cast to and handle data
	switch s.field.Type.ID() {
	case arrow.INT8:
		arr, err := castToTypedArrowArray[*array.Int8](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]int8, s.Len())
		copyDataFromTypedArray(dataValues, arr.Int8Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.INT16:
		arr, err := castToTypedArrowArray[*array.Int16](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]int16, s.Len())
		copyDataFromTypedArray(dataValues, arr.Int16Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.INT32:
		arr, err := castToTypedArrowArray[*array.Int32](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]int32, s.Len())
		copyDataFromTypedArray(dataValues, arr.Int32Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.INT64:
		arr, err := castToTypedArrowArray[*array.Int64](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]int64, s.Len())
		copyDataFromTypedArray(dataValues, arr.Int64Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.STRING:
		arr, err := castToTypedArrowArray[*array.String](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]string, s.Len())
		for i := 0; i < arr.Len(); i++ {
			dataValues[i] = arr.Value(i) // Update Array
		}
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.FLOAT32:
		arr, err := castToTypedArrowArray[*array.Float32](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]float32, s.Len())
		copyDataFromTypedArray(dataValues, arr.Float32Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.FLOAT64:
		arr, err := castToTypedArrowArray[*array.Float64](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]float64, s.Len())
		copyDataFromTypedArray(dataValues, arr.Float64Values())
		return newSeriesElements(dataValues, nullValueArr), nil
	case arrow.BOOL:
		arr, err := castToTypedArrowArray[*array.Boolean](*s)
		if err != nil {
			return SeriesElements{}, err
		}

		dataValues := make([]bool, s.Len())
		for i := 0; i < arr.Len(); i++ {
			dataValues[i] = arr.Value(i) // Update Array
		}
		return newSeriesElements(dataValues, nullValueArr), nil
	default:
		return SeriesElements{}, errors.New("Field misconfiguration error on Series")
	}
}

func (s *basicSeries) Rename(name string) SeriesResult {
	return s.internalCopy(name)
}

func (s *basicSeries) Copy() SeriesResult {
	return s.internalCopy(s.Name())
}

func (s *basicSeries) getArray() arrow.Array {
	return s.array
}

func (s *basicSeries) setReleasable(isReleaseable bool) {
	s.releasable = isReleaseable
}

func (s *basicSeries) Type() arrow.DataType {
	return s.array.DataType()
}

func (s *basicSeries) Name() string {
	return s.field.Name
}

func (s *basicSeries) Min() float64 {
	panic("Unimplemented Min")
}

func (s *basicSeries) Max() float64 {
	panic("Unimplemented Max")
}

func (s *basicSeries) Mode() float64 {
	panic("Unimplemented Mode")
}

func (s *basicSeries) Median() float64 {
	panic("Unimplemented Median")
}

func (s *basicSeries) StdDev() float64 {
	panic("Unimplemented StdDev")
}

func (s *basicSeries) Mean() float64 {
	panic("Unimplemented Mean")
}

func (s *basicSeries) Sum() float64 {
	panic("Unimplemented Sum")
}

func (s *basicSeries) Abs() float64 {
	panic("Unimplemented Abs")
}

func (s *basicSeries) Product() float64 {
	panic("Unimplemented Product")
}

func (s *basicSeries) Quantile(f float32) float64 {
	panic("Unimplemented Quantile")
}

func (s *basicSeries) Variance() float64 {
	panic("Unimplemented Variance")
}

func (s *basicSeries) Mul(value any, dealloc memory.Deallocator) SeriesResult {

	// To-Do, only an example, should really be when we create a copy that is getting returned
	// as a Series result
	if dealloc != nil {
	}
	// (*dealloc).AddResourceToCleanup(&s.array)
	panic("Unimplemented")
}

func (s *basicSeries) Add(value any, dealloc memory.Deallocator) SeriesResult {
	if dealloc == nil {
		panic("Expected Deallocator?")
	}

	// What Type of Values Supported?
	// ints, floats, Series

	var result SeriesResult

	switch data := value.(type) {
	case []int64:
		{
			// Check For Type
			switch s.Type() {
			// To-Do, Wrap in a Method Call For Scalars
			case arrow.PrimitiveTypes.Int8,
				arrow.PrimitiveTypes.Int16,
				arrow.PrimitiveTypes.Int32,
				arrow.PrimitiveTypes.Int64,
				arrow.PrimitiveTypes.Float32,
				arrow.PrimitiveTypes.Float64:

				println("Valid Addition Type")
				println(fmt.Sprintf("[Add] Data P %v", data))

				primitives, err := GetPrimitives[int64](s)
				if err != nil {
					panic(fmt.Sprintf("[Add] Fail Prim Cast"))
				}

				minLength := min(len(primitives), len(data))
				newSlice := make([]int64, minLength)
				for i := 0; i < minLength; i++ {
					newSlice[i] = data[i] + primitives[i]
				}

				// How To Name???
				result = NewReleasableFromSlice(s.Name(), newSlice)

			default:
				panic("Invalid Addition Type")
			}
		}
	default:
		panic("Unsupported Type(s) Being Applied For Series Addition")
	}

	return result
}

func (s *basicSeries) Sub(value any, dealloc memory.Deallocator) SeriesResult {
	panic("Unimplemented")
}

func (s *basicSeries) Div(value any, dealloc memory.Deallocator) SeriesResult {
	panic("Unimplemented")
}

func (s *basicSeries) Append(series *Series) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) ApplyFn(fn interface{}) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) ApplySeries(fn interface{}) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) ApplyFnByIds(fn interface{}, ids []uint32) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (self basicSeries) ApplySeriesByIds(fn interface{}, s *Series, ids []uint32) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) Sort() SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) Subset(ids []uint32) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

func (s *basicSeries) String() string {
	return "Unimplemented"
}

func (s *basicSeries) Len() int {
	return s.array.Len()
}

func (s *basicSeries) IsReleasable() bool {
	return s.releasable
}

func (s *basicSeries) Release() error {
	var err error

	if s.releasable {
		s.array.Release()
	} else {
		err = errors.New("Calling Release on Non-Releasable Series!")
	}

	return err
}

func (s *basicSeries) internalCopy(name string) SeriesResult {
	newSeries := basicSeries{
		releasable: true,
		field:      s.field,
	}
	newSeries.field.Name = name

	// To-Do, Handle All Supported Types
	switch s.field.Type.ID() {
	case arrow.INT8:
		int8Slice, err := GetPrimitives[int8](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewInt8Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(int8Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.INT16:
		int16Slice, err := GetPrimitives[int16](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewInt16Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(int16Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.INT32:
		int32Slice, err := GetPrimitives[int32](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewInt32Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(int32Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.INT64:
		int64Slice, err := GetPrimitives[int64](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewInt64Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(int64Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.FLOAT32:
		float32Slice, err := GetPrimitives[float32](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewFloat32Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(float32Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.FLOAT64:
		float64Slice, err := GetPrimitives[float64](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewFloat64Builder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(float64Slice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.BOOL:
		booleanSlice, err := GetPrimitives[bool](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewBooleanBuilder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(booleanSlice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	case arrow.STRING:
		stringSlice, err := GetPrimitives[string](s)
		if err != nil {
			return newSeriesResult(nil, err)
		}

		bldr := array.NewStringBuilder(arrow_memory.DefaultAllocator)
		bldr.AppendValues(stringSlice, nil)
		defer bldr.Release()
		newSeries.array = bldr.NewArray()
	default:
		return newSeriesResult(nil, errors.New("Unsupported arrow type error!"))
	}

	return newSeriesResult(&newSeries, nil)
}
