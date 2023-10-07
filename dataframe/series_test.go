package dataframe

import (
	"dframe_arrow_research/utils"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
)

// Validation Helper Function
// To-Do, Type should be constrained for arrow primitives
func validateSeriesSuccess[T comparable](t *testing.T, s Series, name string, slice []T, datatype arrow.DataType) {
	if s.Name() != name {
		t.Errorf("Expected %s for name, got %s instead", name, s.Name())
	}

	if s.Type() != datatype {
		t.Errorf("Expected %s for datatype, got %s instead", datatype, s.Type())
	}

	seriesSlice, _ := GetPrimitives[T](s)

	// To-do, switch statement based on type, error handling
	for index, value := range slice {

		// Test Slices
		valueFromSeries := seriesSlice[index]
		if value != valueFromSeries {
			t.Errorf("Internal Series Slice Comparison Failure")
		}

		// Test Individual Access
		valueFromSeries, _ = GetPrimitive[T](index, s)
		if value != valueFromSeries {
			t.Error("Internal individual Data Comparison Failure")
		}
	}
}

// Tests The Creation of a Series of Supported Types
func TestSeriesCreate_Success(t *testing.T) {

	var numRows int = 5000

	seriesInt8Name := "Int8 Series"
	seriesInt8Data := utils.GenerateInt8Slice(numRows)
	seriesInt8 := NewReleasableFromSlice(seriesInt8Name, seriesInt8Data).Get()
	defer seriesInt8.Release()
	validateSeriesSuccess(t, seriesInt8, seriesInt8Name, seriesInt8Data, arrow.PrimitiveTypes.Int8)

	seriesInt16Name := "Int16 Series"
	seriesInt16Data := utils.GenerateInt16Slice(numRows)
	seriesInt16 := NewReleasableFromSlice(seriesInt16Name, seriesInt16Data).Get()
	defer seriesInt16.Release()
	validateSeriesSuccess(t, seriesInt16, seriesInt16Name, seriesInt16Data, arrow.PrimitiveTypes.Int16)

	seriesInt32Name := "Int32 Series"
	seriesInt32Data := utils.GenerateInt32Slice(numRows)
	seriesInt32 := NewReleasableFromSlice(seriesInt32Name, seriesInt32Data).Get()
	defer seriesInt32.Release()
	validateSeriesSuccess(t, seriesInt32, seriesInt32Name, seriesInt32Data, arrow.PrimitiveTypes.Int32)

	seriesInt32CopyTest := seriesInt32.Copy().Get()
	validateSeriesSuccess(t, seriesInt32CopyTest, seriesInt32Name, seriesInt32Data, arrow.PrimitiveTypes.Int32)

	seriesInt64Name := "Int64 Series"
	seriesInt64Data := utils.GenerateInt64Slice(numRows)
	seriesInt64 := NewReleasableFromSlice(seriesInt64Name, seriesInt64Data).Get()
	defer seriesInt64.Release()
	validateSeriesSuccess(t, seriesInt64, seriesInt64Name, seriesInt64Data, arrow.PrimitiveTypes.Int64)

	seriesFloat32Name := "Float32 Series"
	seriesFloat32Data := utils.GenerateFloat32Slice(numRows)
	seriesFloat32 := NewReleasableFromSlice(seriesFloat32Name, seriesFloat32Data).Get()
	defer seriesFloat32.Release()
	validateSeriesSuccess(t, seriesFloat32, seriesFloat32Name, seriesFloat32Data, arrow.PrimitiveTypes.Float32)

	seriesFloat64Name := "Float64 Series"
	seriesFloat64Data := utils.GenerateFloat64Slice(numRows)
	seriesFloat64 := NewReleasableFromSlice(seriesFloat64Name, seriesFloat64Data).Get()
	defer seriesFloat64.Release()
	validateSeriesSuccess(t, seriesFloat64, seriesFloat64Name, seriesFloat64Data, arrow.PrimitiveTypes.Float64)

	seriesStringName := "String Series"
	seriesStringData := utils.GenerateStringSlice(numRows)
	seriesString := NewReleasableFromSlice(seriesStringName, seriesStringData).Get()
	defer seriesString.Release()
	validateSeriesSuccess(t, seriesString, seriesStringName, seriesStringData, arrow.BinaryTypes.String)

	seriesBoolName := "Bool Series"
	seriesBoolData := utils.GenerateBoolSlice(numRows)
	seriesBool := NewReleasableFromSlice(seriesBoolName, seriesBoolData).Get()
	defer seriesBool.Release()
	validateSeriesSuccess(t, seriesBool, seriesBoolName, seriesBoolData, arrow.FixedWidthTypes.Boolean)

	// To-Do, Date Series Via time.Time arrays

}
