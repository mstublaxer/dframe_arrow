package utils

import (
	"math/rand"
	"strconv"
)

// ////////// Slice Data Generators //////////////
func GenerateInt8Slice(n int) (data []int8) {
	for i := 0; i < n; i++ {
		data = append(data, int8(rand.Intn(1<<8)))
	}
	return data
}

func GenerateInt16Slice(n int) (data []int16) {
	for i := 0; i < n; i++ {
		data = append(data, int16(rand.Intn(1<<16)))
	}
	return data
}

func GenerateInt32Slice(n int) (data []int32) {
	for i := 0; i < n; i++ {
		data = append(data, rand.Int31())
	}
	return data
}

func GenerateInt64Slice(n int) (data []int64) {
	for i := 0; i < n; i++ {
		data = append(data, rand.Int63())
	}
	return data
}

func GenerateFloat32Slice(n int) (data []float32) {
	for i := 0; i < n; i++ {
		data = append(data, rand.Float32())
	}
	return data
}

func GenerateFloat64Slice(n int) (data []float64) {
	for i := 0; i < n; i++ {
		data = append(data, rand.Float64())
	}
	return data
}

func GenerateStringSlice(n int) (data []string) {
	for i := 0; i < n; i++ {
		data = append(data, strconv.Itoa(rand.Int()))
	}
	return data
}

func GenerateBoolSlice(n int) (data []bool) {
	var base bool = false
	var randomInt int

	for i := 0; i < n; i++ {
		randomInt = rand.Intn(2)
		base = false
		if randomInt == 1 {
			base = true
		}
		data = append(data, base)
	}
	return
}
