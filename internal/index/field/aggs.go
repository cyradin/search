package field

import (
	"container/heap"
	"context"

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

type rangeAggRange[T NumericConstraint] struct {
	Key  string
	From *T
	To   *T
}

type RangeBucket struct {
	Key  string          `json:"key"`
	From interface{}     `json:"from"`
	To   interface{}     `json:"to"`
	Docs *roaring.Bitmap `json:"docCount"`
}

type RangeAggResult struct {
	Buckets []RangeBucket
}

func rangeAgg[T NumericConstraint](ctx context.Context, docs *roaring.Bitmap, data *docValues[T], ranges []rangeAggRange[T]) RangeAggResult {
	result := RangeAggResult{
		Buckets: make([]RangeBucket, 0, len(ranges)),
	}

	for _, r := range ranges {
		if docs == nil || docs.IsEmpty() || (r.From == nil && r.To == nil) {
			result.Buckets = append(result.Buckets, RangeBucket{
				From: any(r.From),
				To:   any(r.To),
				Key:  r.Key,
				Docs: roaring.New(),
			})
			continue
		}

		d := rangeQuery(ctx, data, r.From, r.To, true, true)
		d.And(docs)
		result.Buckets = append(result.Buckets, RangeBucket{
			From: r.From,
			To:   r.To,
			Key:  r.Key,
			Docs: d,
		})
	}

	return result
}
