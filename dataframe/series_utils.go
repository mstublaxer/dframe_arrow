package dataframe

import (
	"dframe_arrow_research/types"
	"dframe_arrow_research/utils"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// To-Do, constrain type to only allowed types
// Data Wrapper
// To-Do, immutability to avoid race conditions? return copied data
func GetElement[T comparable](i int, s Series) (types.Element[T], error) {
	seriesElement, err := s.Element(i)

	var t T
	if err != nil {
		return types.Element[T]{Val: t, Null: true}, err
	}

	val, ok := seriesElement.Val.(T)
	if !ok {
		return types.Element[T]{Val: t, Null: true}, errors.New("Invalid Element Type!")
	}

	return types.Element[T]{Val: val, Null: seriesElement.Null}, nil
}

// To-Do, constrain type to only allowed types
// Data Wrapper
// Should this return a copy or ?
func GetElements[T comparable](s Series) ([]types.Element[T], error) {
	arr := make([]types.Element[T], s.Len())
	for i := 0; i < s.Len(); i++ {
		newVal, err := GetElement[T](i, s)
		if err != nil {
			return nil, err
		}
		arr[i] = newVal
	}

	return arr, nil
}

// To-Do, constrain type to only allowed types
// Used if user only cares about the direct primitive value(regardless if null)
func GetPrimitive[T comparable](i int, s Series) (T, error) {
	seriesElement, err := s.Element(i)

	var t T
	if err != nil {
		return t, err
	}

	val, ok := seriesElement.Val.(T)
	if !ok {
		return t, errors.New("Invalid Primitive Type!")
	}

	return val, nil
}

// To-do, explore making more efficient
// Instead of getting each element individual, can I do at once?x
func GetPrimitives[T comparable](s Series) ([]T, error) {
	arr := make([]T, s.Len())
	for i := 0; i < s.Len(); i++ {
		newVal, err := GetPrimitive[T](i, s)
		if err != nil {
			return nil, err
		}
		arr[i] = newVal
	}

	return arr, nil
}

// To-Do, Implement
func ConcatenateSeries(allSeries ...*Series) SeriesResult {
	panic("Unimplemented")
	return SeriesResult{}
}

// Casts a Series array to a typed array
func castToTypedArrowArray[T comparable](s basicSeries) (T, error) {
	var t T
	arr, ok := s.array.(T)
	if !ok {
		return t, errors.New(fmt.Sprintf("Could not get %s Array", s.field.Type.Name()))
	}
	return arr, nil
}

// Copies Data From a Typed Array into an any slice
func copyDataFromTypedArray[T comparable](dst []T, src []T) {
	for i, v := range src {
		dst[i] = v
	}
}

// Given a Data Populated Arrow Array Builder, sets up a series with the array/field
func setupBasicSeries(series *basicSeries, bldr array.Builder, name string, datatype arrow.DataType) {
	newArray := bldr.NewArray()
	series.field = arrow.Field{
		Name: name,
		Type: datatype,
	}

	series.array = newArray
}

// /////////////////////// Series Generators //////////////////////// //
func generateInt32Series(name string, numRows int) SeriesResult {
	return NewReleasableFromSlice(name, utils.GenerateInt32Slice(numRows))
}

func generateBoolSeries(name string, numRows int) SeriesResult {
	return NewReleasableFromSlice(name, utils.GenerateBoolSlice(numRows))
}

func generateFloat64Series(name string, numRows int) SeriesResult {
	return NewReleasableFromSlice(name, utils.GenerateFloat64Slice(numRows))
}

func generateStringSeries(name string, numRows int) SeriesResult {
	return NewReleasableFromSlice(name, utils.GenerateStringSlice(numRows))
}
