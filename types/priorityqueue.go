package types

// Interface Used Along Base Priority Queue
type SortableStruct interface {
	Value() int
}

// Generic Baseline Cue For Sorting Elements as they come in
// Usage is to maintain order of elements as they arrive
type BasePriorityQueue[T SortableStruct] []T

func (tq BasePriorityQueue[T]) Len() int           { return len(tq) }
func (tq BasePriorityQueue[T]) Less(i, j int) bool { return tq[i].Value() < tq[j].Value() }
func (tq BasePriorityQueue[T]) Swap(i, j int)      { tq[i], tq[j] = tq[j], tq[i] }

func (tq *BasePriorityQueue[T]) Push(x interface{}) {
	*tq = append(*tq, x.(T))
}

func (tq *BasePriorityQueue[T]) Pop() interface{} {
	old := *tq
	n := len(old)
	x := old[n-1]
	*tq = old[0 : n-1]
	return x
}
