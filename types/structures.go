package types

// Represents a single element that may or may not be null
type Element[T comparable] struct {
	Val  T
	Null bool
}

// Represents numerous potential elements being null
type Elements struct {
	Values       any
	IsNullValues []bool
}
