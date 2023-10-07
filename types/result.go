package types

// Result Wrapper For Types that may return with errors
type Result[T any] struct {
	val T
	err error
}

func NewResult[T any](val T, err error) Result[T] {
	return Result[T]{val: val, err: err}
}

func (s Result[T]) Get() T {
	return s.val
}

func (s Result[T]) GetErr() error {
	return s.err
}
