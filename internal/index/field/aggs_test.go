package field

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

func Test_keyValueHeap(t *testing.T) {
	bm1 := roaring.New()
	bm1.Add(1)

	bm2 := roaring.New()
	bm2.Add(1)
	bm2.Add(2)

	bm3 := roaring.New()
	bm3.Add(1)
	bm3.Add(2)
	bm3.Add(3)

	bm4 := roaring.New()
	bm4.Add(1)
	bm4.Add(2)
	bm4.Add(3)
	bm4.Add(4)

	h := &termHeap[string]{
		keyValue[string]{Key: "1", Docs: bm1},
		keyValue[string]{Key: "2", Docs: bm2},
		keyValue[string]{Key: "3", Docs: bm3},
	}
	heap.Init(h)
	heap.Push(h, keyValue[string]{Key: "4", Docs: bm4})

	v := heap.Pop(h)
	require.Equal(t, keyValue[string]{Key: "4", Docs: bm4}, v)
	require.Equal(t, 3, h.Len())
}

func Benchmark_termAgg(b *testing.B) {
	allCounts := [][]int{
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

	for _, counts := range allCounts {
		docs := roaring.New()
		data := newDocValues[int32]()
		for i := 0; i < counts[0]; i++ {
			v1 := i%counts[1] + 1
			v2 := i%counts[1] + 2
			v3 := i%counts[1] + 3

			docs.Add(uint32(i))

			data.Add(uint32(i), int32(v1))
			data.Add(uint32(i), int32(v2))
			data.Add(uint32(i), int32(v3))
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
		data := newDocValues[int32]()

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

		data := newDocValues[int32]()
		data.Add(1, 1)
		data.Add(1, 2)
		data.Add(1, 3)
		data.Add(1, 4)
		data.Add(2, 1)
		data.Add(2, 2)
		data.Add(2, 3)
		data.Add(3, 1)
		data.Add(3, 2)
		data.Add(4, 1)

		result := termAgg(docs, data, 20)

		require.Len(t, result.Buckets, 4)
		for i := 0; i < 4; i++ {
			key := int32(i + 1)
			require.Equal(t, result.Buckets[int32(i)].Key, key)
			require.ElementsMatch(t, result.Buckets[int32(i)].Docs.ToArray(), data.DocsByValue(key).ToArray())
		}
	})
}
