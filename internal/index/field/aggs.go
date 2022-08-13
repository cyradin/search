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

var _ heap.Interface = (*termHeap[bool])(nil)

type keyValue[T comparable] struct {
	Key  T
	Docs *roaring.Bitmap
}

type termHeap[T comparable] []keyValue[T]

func (h termHeap[T]) Len() int { return len(h) }
func (h termHeap[T]) Less(i, j int) bool {
	return h[i].Docs.GetCardinality() > h[j].Docs.GetCardinality()
}
func (h termHeap[T]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *termHeap[T]) Push(x any) {
	*h = append(*h, x.(keyValue[T]))
}

func (h *termHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TermBucket struct {
	Key  interface{}
	Docs *roaring.Bitmap
}

type TermAggResult struct {
	Buckets []TermBucket
}

func termAgg[T Simple](docs *roaring.Bitmap, data *docValues[T], size int) TermAggResult {
	heapData := make(termHeap[T], 0, size)
	for _, v := range data.List {
		valueDocs := data.DocsByValue(v).Clone()
		valueDocs.And(docs)

		if valueDocs.IsEmpty() {
			continue
		}

		heapData = append(heapData, keyValue[T]{Key: v, Docs: valueDocs})
	}
	heap.Init(&heapData)

	buckets := make([]TermBucket, minInt(size, len(heapData)))
	for i := range buckets {
		v := heap.Pop(&heapData).(keyValue[T])

		buckets[i] = TermBucket{
			Key:  v.Key,
			Docs: v.Docs,
		}
	}

	return TermAggResult{
		Buckets: buckets,
	}
}
