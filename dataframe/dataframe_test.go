package dataframe

import (
	"dframe_arrow_research/utils"
	"testing"
)

func validateSlices[T comparable](seriesData any, baseData []T) bool {
	typedSeriesData, ok := seriesData.([]T)
	if !ok {
		return false
	}

	validSlice := true
	for index, data := range baseData {
		if typedSeriesData[index] != data {
			validSlice = false
			break
		}
	}
	return validSlice
}

func TestAddColumns(t *testing.T) {

	// To-Do, Automate and Wrap For All Types
	numRows := 100
	df := New()
	defer df.Release()

	seriesInt32Name := "Int32 Series"
	seriesInt32Data := utils.GenerateInt32Slice(numRows)
	seriesInt32Result := NewReleasableFromSlice(seriesInt32Name, seriesInt32Data)
	err := df.AddColumn(seriesInt32Result.Get())
	if err != nil {
		panic("Error Should Not Be Nil!")
	}

	seriesBoolName := "Bool Series"
	seriesBoolData := utils.GenerateBoolSlice(numRows)
	seriesBoolResult := NewReleasableFromSlice(seriesBoolName, seriesBoolData)
	err = df.AddColumn(seriesBoolResult.Get())
	if err != nil {
		panic("Error Should Not Be Nil!")
	}

	seriesFloatName := "Float64 Series"
	seriesFloatData := utils.GenerateFloat64Slice(numRows)
	seriesFloat64Result := NewReleasableFromSlice(seriesFloatName, seriesFloatData)
	err = df.AddColumn(seriesFloat64Result.Get())
	if err != nil {
		panic("Error Should Not Be Nil!")
	}

	seriesStringName := "String Series"
	seriesStringData := utils.GenerateStringSlice(numRows)
	seriesStringResult := NewReleasableFromSlice(seriesStringName, seriesStringData)
	err = df.AddColumn(seriesStringResult.Get())
	if err != nil {
		panic("Error Should Not Be Nil!")
	}

	rowCount, columnCount := df.Shape()
	expectedRowCount := numRows
	expectedColumnCount := 4

	if rowCount != int64(expectedRowCount) {
		t.Errorf("Expected %d, but got %d", expectedRowCount, rowCount)
	}

	if columnCount != int64(expectedColumnCount) {
		t.Errorf("Expected %d, but got %d", expectedColumnCount, columnCount)
	}

	toCopy := true
	copiedInt32SeriesResult := df.ColumnC(seriesInt32Name, toCopy)
	copiedFloat64SeriesResult := df.ColumnC(seriesFloatName, toCopy)
	copiedBoolSeriesResult := df.ColumnC(seriesBoolName, toCopy)
	copiedStringSeriesResult := df.ColumnC(seriesStringName, toCopy)

	copiedInt32PrimitiveElements, _ := copiedInt32SeriesResult.Get().Elements()
	validInt32Series := validateSlices[int32](copiedInt32PrimitiveElements.Values, seriesInt32Data)
	if !validInt32Series {
		t.Errorf("Int32 Data Doesn't Match Passed Data!")
	}

	copiedFloatPrimitiveElements, _ := copiedFloat64SeriesResult.Get().Elements()
	validFloat64Series := validateSlices[float64](copiedFloatPrimitiveElements.Values, seriesFloatData)
	if !validFloat64Series {
		t.Errorf("Float64 Data Doesn't Match Passed Data!")
	}

	copiedBoolPrimitiveElements, _ := copiedBoolSeriesResult.Get().Elements()
	validBoolSeries := validateSlices[bool](copiedBoolPrimitiveElements.Values, seriesBoolData)
	if !validBoolSeries {
		t.Errorf("Bool Data Doesnt Match Passed Data!")
	}

	copiedStringPrimitiveElements, _ := copiedStringSeriesResult.Get().Elements()
	validStringSeries := validateSlices[string](copiedStringPrimitiveElements.Values, seriesStringData)
	if !validStringSeries {
		t.Errorf("String Data Doesnt Match Passed Data!")
	}
}

func TestDataFrameCuts(t *testing.T) {
	df := New()
	defer df.Release()
	numRows := 100
	expectedRowCount := 2
	expectedColumnCount := 5

	df.AddSeries(generateInt32Series("Test Int Series 1", numRows).Get())
	df.AddSeries(generateInt32Series("Test Int Series 2", numRows).Get())
	df.AddSeries(generateInt32Series("Test Int Series 3", numRows).Get())
	df.AddSeries(generateInt32Series("Test Int Series 4", numRows).Get())
	df.AddSeries(generateInt32Series("Test Int Series 5", numRows).Get())

	// Head
	// To-Do, Add Deallocator/Update Func
	headDf := df.Head(2, nil)
	defer headDf.Get().Release()
	headRowCount, headColumnCount := headDf.Get().Shape()

	if headRowCount != int64(expectedRowCount) {
		t.Errorf("Expected %d, but got %d", expectedRowCount, headRowCount)
	}

	if headColumnCount != int64(expectedColumnCount) {
		t.Errorf("Expected %d, but got %d", expectedColumnCount, headColumnCount)
	}

	// Tail
	// To-Do, Add Deallocator/Update Func
	tailDf := df.Tail(2, nil)
	defer tailDf.Get().Release()
	tailRowCount, tailColumnCount := tailDf.Get().Shape()

	if tailRowCount != int64(expectedRowCount) {
		t.Errorf("Expected %d, but got %d", expectedRowCount, tailRowCount)
	}

	if tailColumnCount != int64(expectedColumnCount) {
		t.Errorf("Expected %d, but got %d", expectedColumnCount, tailColumnCount)
	}
}

func TestFilterOperation(t *testing.T) {

	// Basic Filters
	df := New()
	defer df.Release()

	seriesInt32BaseName := "Int32 Base Series"
	seriesInt32BaseData := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	seriesInt32BasicResult := NewReleasableFromSlice(seriesInt32BaseName, seriesInt32BaseData)

	seriesInt32SegName := "Int32 Seg Series"
	seriesInt32SegData := []int32{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 9999}
	seriesInt32SegResult := NewReleasableFromSlice(seriesInt32SegName, seriesInt32SegData)

	df.AddColumn(seriesInt32SegResult.Get())
	err := df.AddColumn(seriesInt32BasicResult.Get())
	if err != nil {
		panic("Fail")
	}

	// Filter 1
	filteredDf1Result := df.Filter(F(seriesInt32BaseName, Gt, 5).And(F(seriesInt32BaseName, Less, 9)).Or(F(seriesInt32BaseName, Less, 3)))
	filteredDf1 := filteredDf1Result.Get()
	defer filteredDf1.Release()

	columnElements, err := filteredDf1.ColumnC(seriesInt32BaseName, false).Get().Elements()
	isColumnValid := validateSlices[int32](columnElements.Values, []int32{1, 2, 6, 7, 8})
	if !isColumnValid {
		t.Errorf("%s Column Failed Filter Check for Filter1!", seriesInt32BaseName)
	}

	columnElements, err = filteredDf1.ColumnC(seriesInt32SegName, false).Get().Elements()
	isColumnValid = validateSlices[int32](columnElements.Values, []int32{1000, 2000, 6000, 7000, 8000})
	if !isColumnValid {
		t.Errorf("%s Column Failed Filter Check for Filter1!", seriesInt32SegName)
	}

	// Filter 2
	baseCondition := F(seriesInt32SegName, ">=", 9000).Or(F(seriesInt32BaseName, ">", 2).And(F(seriesInt32BaseName, "<", 5)))
	orCondition := F(seriesInt32BaseName, "==", 7)
	filteredDf2Result := df.Filter(baseCondition.Or(orCondition))
	filteredDf2 := filteredDf2Result.Get()
	defer filteredDf2.Release()

	columnElements, err = filteredDf2.ColumnC(seriesInt32BaseName, false).Get().Elements()
	isColumnValid = validateSlices[int32](columnElements.Values, []int32{3, 4, 7, 9, 10})
	if !isColumnValid {
		t.Errorf("%s Column Failed Filter Check for Filter2!", seriesInt32BaseName)
	}

	columnElements, err = filteredDf2.ColumnC(seriesInt32SegName, false).Get().Elements()
	isColumnValid = validateSlices[int32](columnElements.Values, []int32{3000, 4000, 7000, 9000, 9999})
	if !isColumnValid {
		t.Errorf("%s Column Failed Filter Check for Filter2!", seriesInt32SegName)
	}

	// Advanced Filter
	// To-Do, Handle Numerous Types(Bool, String, Date, Floats, Numerous Ints)
}

func TestMismatchedRowCount(t *testing.T) {

}

// To-Do, Add Checks For General Errors(Unsupported Operations on Types etc)

func TestBasicDataFrame(t *testing.T) {
	// Read From CSV

	// Compare Two Series (Should Be Compatible Types, Define Matching Types?,
	// could easily use a static map or function)

	// Get Dimensionality Info

	// Convert The DataFrame to a String

	// Get the Mean Value

	// Merge

	// Replace Function(Can be used to cleanup stuff like math.NaN)

	// Multiply, Add, Subtract, Divide -> Return a Series ?
	// Append That Same Series to a new column ?

	// Appending a Series with an Existing Column Name, replace instead of error?*/
}
