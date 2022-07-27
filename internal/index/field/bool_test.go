package field

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

var benchmarkCounts = []int{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000}

func Benchmark_Bool_Term_Values_In_A_Row(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		f := NewBool()
		for i := 0; i < cnt; i++ {
			f.Add(uint32(i), true)
		}

		ctx := context.Background()
		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.Term(ctx, true)
			}
		})
	}
}

func Benchmark_Bool_Term_Values_In_A_Row_Plus_1000(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		f := NewBool()
		for i := 0; i < cnt; i++ {
			f.Add(uint32(i+1000), true)
		}

		ctx := context.Background()
		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.Term(ctx, true)
			}
		})
	}
}

func Benchmark_Bool_Term_Values_In_A_Row_Even(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		f := NewBool()
		for i := 0; i < cnt; i++ {
			f.Add(uint32(i*2), true)
		}

		ctx := context.Background()
		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.Term(ctx, true)
			}
		})
	}
}

func Benchmark_Bool_Term_Values_Random(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		f := NewBool()
		for i := 0; i < cnt; i++ {
			f.Add(rand.Uint32(), true)
		}

		ctx := context.Background()
		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.Term(ctx, true)
			}
		})
	}
}

func Benchmark_Bool_Term_Values_Random_Sorted(b *testing.B) {
	for _, cnt := range benchmarkCounts {
		f := NewBool()

		values := make([]uint32, cnt)
		for i := 0; i < cnt; i++ {
			values[i] = rand.Uint32()
		}
		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})

		for _, v := range values {
			f.Add(v, true)
		}

		ctx := context.Background()
		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f.Term(ctx, true)
			}
		})
	}
}

func Test_Bool_Add(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		field := NewBool()
		field.Add(1, true)
		field.Add(1, false)
		field.Add(2, true)

		require.EqualValues(t, 2, field.dataTrue.GetCardinality())
		require.EqualValues(t, 1, field.dataFalse.GetCardinality())
		require.True(t, field.dataTrue.Contains(1))
		require.True(t, field.dataTrue.Contains(2))
		require.True(t, field.dataFalse.Contains(1))
		require.False(t, field.dataFalse.Contains(2))
	})
	t.Run("string", func(t *testing.T) {
		field := NewBool()
		field.Add(1, "qwe")

		require.EqualValues(t, 0, field.dataTrue.GetCardinality())
		require.EqualValues(t, 0, field.dataFalse.GetCardinality())
	})
}

func Test_Bool_Term(t *testing.T) {
	field := NewBool()
	field.Add(1, true)

	result := field.Term(context.Background(), true)
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.Term(context.Background(), false)
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_Bool_Delete(t *testing.T) {
	field := NewBool()
	field.Add(1, true)
	field.Add(1, false)
	field.Add(2, false)

	field.Delete(2)
	require.EqualValues(t, 1, field.dataTrue.GetCardinality())
	require.EqualValues(t, 1, field.dataFalse.GetCardinality())

	field.Delete(1)
	require.EqualValues(t, 0, field.dataTrue.GetCardinality())
	require.EqualValues(t, 0, field.dataFalse.GetCardinality())
}

func Test_Bool_Data(t *testing.T) {
	field := NewBool()
	field.Add(1, true)
	field.Add(1, false)
	field.Add(2, false)

	result := field.Data(1)
	require.EqualValues(t, []interface{}{true, false}, result)

	result = field.Data(2)
	require.EqualValues(t, []interface{}{false}, result)
}

func Test_Bool_Marshal(t *testing.T) {
	field := NewBool()
	field.Add(1, true)
	field.Add(1, false)
	field.Add(2, true)

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := NewBool()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.dataTrue.Contains(1))
	require.True(t, field2.dataFalse.Contains(1))
	require.True(t, field2.dataTrue.Contains(2))
}
