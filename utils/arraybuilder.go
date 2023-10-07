package utils

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v12/arrow/memory"
)

func NewArrowArray(data any) (arrow.Array, error) {
	if data == nil {
		return nil, errors.New("Cannot Create Data From Nil")
	}

	var newBuilder array.Builder

	switch values := data.(type) {
	case []int8:
		{
			bldr := array.NewInt8Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []int16:
		{
			bldr := array.NewInt16Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []int32:
		{
			bldr := array.NewInt32Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []int64:
		{
			bldr := array.NewInt64Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []float32:
		{
			bldr := array.NewFloat32Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []float64:
		{
			bldr := array.NewFloat64Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []string:
		{
			bldr := array.NewStringBuilder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []bool:
		{
			bldr := array.NewBooleanBuilder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	case []arrow.Date32:
		{
			bldr := array.NewDate32Builder(arrow_memory.DefaultAllocator)
			bldr.AppendValues(values, nil)
			newBuilder = bldr
		}
	default:
		return nil, errors.New("Unsupported Data Provided For Creating Slice")
	}

	if newBuilder == nil {
		panic("Nil Builder Exception!")
	}

	defer newBuilder.Release()
	return newBuilder.NewArray(), nil
}

func CreateSliceFromRecords[T comparable](records []arrow.Record, colNum int) ([]T, error) {
	newSlice := make([]T, 0)
	if len(records) == 0 {
		return newSlice, errors.New("Empty Records Array Received")
	}

	// Check DataType Here, Create Function that Can Take an array and get slice data from it
	firstRecord := records[0]
	if colNum > int(firstRecord.NumCols()) {
		return newSlice, errors.New("Invalid Column Number supplied for Slice Conversion")
	}

	for i := 0; i < len(records); i++ {
		convertedSlice, err := CreateSliceFromArrowArr[T](records[i].Column(colNum))
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Failed to convert %v column in record %v", records[i].ColumnName(colNum), i))
		}
		newSlice = append(newSlice, convertedSlice...)
	}

	return newSlice, nil
}

func CreateSliceFromArrowArr[T comparable](arr arrow.Array) ([]T, error) {

	failedConvertErrMsg := fmt.Sprintf("Failed to Create %v Converter", arr.DataType().Name())
	var values interface{}

	switch arr.DataType() {
	case arrow.PrimitiveTypes.Int8:
		{
			typedArr, ok := arr.(*array.Int8)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Int8Values()
		}
	case arrow.PrimitiveTypes.Int16:
		{
			typedArr, ok := arr.(*array.Int16)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Int16Values()
		}
	case arrow.PrimitiveTypes.Int32:
		{
			typedArr, ok := arr.(*array.Int32)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Int32Values()
		}
	case arrow.PrimitiveTypes.Int64:
		{
			typedArr, ok := arr.(*array.Int64)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Int64Values()
		}
	case arrow.PrimitiveTypes.Float32:
		{
			typedArr, ok := arr.(*array.Float32)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Float32Values()
		}
	case arrow.PrimitiveTypes.Float64:
		{
			typedArr, ok := arr.(*array.Float64)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			values = typedArr.Float64Values()
		}
	case arrow.BinaryTypes.String:
		{
			typedArr, ok := arr.(*array.String)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}

			typedValues := make([]string, typedArr.Len())
			for i := 0; i < arr.Len(); i++ {
				typedValues[i] = typedArr.Value(i)
			}
			values = typedValues
		}
	case arrow.FixedWidthTypes.Boolean:
		{
			typedArr, ok := arr.(*array.Boolean)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			typedValues := make([]bool, typedArr.Len())
			for i := 0; i < arr.Len(); i++ {
				typedValues[i] = typedArr.Value(i)
			}
			values = typedValues
		}
	case arrow.PrimitiveTypes.Date32:
		{
			typedArr, ok := arr.(*array.Date32)
			if !ok {
				return nil, errors.New(failedConvertErrMsg)
			}
			typedValues := typedArr.Date32Values()
			values = typedValues
		}

	default:
		panic(fmt.Sprintf("Attempted to create Slice from unsupported data type: %v", arr.DataType().Name()))
	}

	outputArr, ok := values.([]T)
	if !ok {
		return nil, errors.New(failedConvertErrMsg)
	}

	return outputArr, nil
}
