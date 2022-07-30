package field

import (
	"container/heap"

	"github.com/RoaringBitmap/roaring"
)

func minInt(v1 int, v2 int) int {
	if v1 > v2 {
		return v2
	}

	return v1
}

var _ heap.Interface = (*keyValueHeap[bool])(nil)

type keyValue[T comparable] struct {
	Key   T
	Value int
}

type keyValueHeap[T comparable] []keyValue[T]

func (h keyValueHeap[T]) Len() int           { return len(h) }
func (h keyValueHeap[T]) Less(i, j int) bool { return h[i].Value > h[j].Value }
func (h keyValueHeap[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *keyValueHeap[T]) Push(x any) {
	*h = append(*h, x.(keyValue[T]))
}

func (h *keyValueHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TermBucket struct {
	Key      interface{}
	DocCount int
}

type TermAggResult struct {
	Buckets []TermBucket
}

func termAgg[T comparable](docs *roaring.Bitmap, data docValues[T], size int) TermAggResult {
	values := make(map[T]int)
	for _, id := range docs.ToArray() {
		for v := range data[id] {
			values[v]++
		}
	}

	heapData := make(keyValueHeap[T], 0, len(values))
	for k, v := range values {
		heapData = append(heapData, keyValue[T]{Key: k, Value: v})
	}
	heap.Init(&heapData)

	buckets := make([]TermBucket, minInt(size, len(heapData)))
	for i := range buckets {
		v := heap.Pop(&heapData).(keyValue[T])

		buckets[i] = TermBucket{
			Key:      v.Key,
			DocCount: v.Value,
		}
	}

	return TermAggResult{
		Buckets: buckets,
	}
}
