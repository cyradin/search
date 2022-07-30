package field

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

func Test_keyValueHeap(t *testing.T) {
	h := &keyValueHeap[string]{
		keyValue[string]{Key: "1", Value: 1},
		keyValue[string]{Key: "2", Value: 2},
		keyValue[string]{Key: "3", Value: 3},
	}
	heap.Init(h)
	heap.Push(h, keyValue[string]{Key: "4", Value: 4})

	v := heap.Pop(h)
	require.Equal(t, keyValue[string]{Key: "4", Value: 4}, v)
	require.Equal(t, 3, h.Len())
}

var termAggBenchmarkCounts = [][]int{
	{10, 10},
	{100, 10},
	{1000, 50},
	{10000, 100},
	{100000, 10},
	{100000, 50},
	{100000, 100},
	{100000, 500},
	{1000000, 1000},
	{10000000, 10},
	{10000000, 100},
	{10000000, 1000},
}

func Benchmark_termAgg(b *testing.B) {
	for _, counts := range termAggBenchmarkCounts {
		docs := roaring.New()
		data := make(docValues[int])
		for i := 0; i < counts[0]; i++ {
			v1 := i%counts[1] + 1
			v2 := i%counts[1] + 2
			v3 := i%counts[1] + 3

			docs.Add(uint32(i))
			data[uint32(i)] = docValue[int]{v1: {}, v2: {}, v3: {}}
		}

		b.Run(fmt.Sprintf("docs_%d_cardinality_%d", counts[0], counts[1]), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				termAgg(docs, data, 20)
			}
		})
	}

}

func Test_termAgg(t *testing.T) {
	t.Run("empty docs", func(t *testing.T) {
		docs := roaring.New()
		data := make(docValues[int])

		result := termAgg(docs, data, 20)
		require.Len(t, result.Buckets, 0)
	})

	t.Run("non-empty docs", func(t *testing.T) {
		docs := roaring.New()
		docs.Add(1)
		docs.Add(2)
		docs.Add(3)
		docs.Add(4)
		docs.Add(5)

		data := docValues[int]{
			1: {1: {}, 2: {}, 3: {}, 4: {}},
			2: {1: {}, 2: {}, 3: {}},
			3: {1: {}, 2: {}},
			4: {1: {}},
		}

		result := termAgg(docs, data, 20)

		require.Equal(t, []TermBucket{
			{Key: 1, DocCount: 4},
			{Key: 2, DocCount: 3},
			{Key: 3, DocCount: 2},
			{Key: 4, DocCount: 1},
		}, result)
	})
}
