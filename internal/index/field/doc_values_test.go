package field

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Benchmark_docValues_listAdd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := newDocValues[int32]()
		for j := 0; j < 100; j++ {
			v.listAdd(int32(j))
		}
	}
}

func Test_docValues_IsEmpty(t *testing.T) {
	v := newDocValues[int32]()
	require.True(t, v.IsEmpty())

	v.Add(1, 1)
	require.False(t, v.IsEmpty())
}

func Test_docValues_Cardinality(t *testing.T) {
	v := newDocValues[int32]()
	require.Equal(t, 0, v.Cardinality())

	v.Add(1, 1)
	require.Equal(t, 1, v.Cardinality())

	v.Add(1, 1)
	require.Equal(t, 1, v.Cardinality())

	v.Add(1, 2)
	require.Equal(t, 2, v.Cardinality())
}

func Test_docValues_ContainsDoc(t *testing.T) {
	v := newDocValues[int32]()
	require.False(t, v.ContainsDoc(1))

	v.Add(1, 1)
	require.True(t, v.ContainsDoc(1))
	require.False(t, v.ContainsDoc(2))
}

func Test_docValues_ContainsDocValue(t *testing.T) {
	v := newDocValues[int32]()
	require.False(t, v.ContainsDocValue(1, 1))

	v.Add(1, 1)
	require.True(t, v.ContainsDocValue(1, 1))
	require.False(t, v.ContainsDocValue(1, 2))
}

func Test_docValues_ValuesByDoc(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	require.ElementsMatch(t, []int32{1}, v.ValuesByDoc(1))
	v.Add(1, 2)
	require.ElementsMatch(t, []int32{1, 2}, v.ValuesByDoc(1))
}

func Test_docValues_DocsByIndex(t *testing.T) {
	v := newDocValues[int32]()

	require.Empty(t, v.DocsByIndex(-1).ToArray())
	require.Empty(t, v.DocsByIndex(1).ToArray())

	v.Add(1, 1)
	require.ElementsMatch(t, []uint32{1}, v.DocsByIndex(0).ToArray())

	v.Add(1, 2)
	require.ElementsMatch(t, []uint32{1}, v.DocsByIndex(0).ToArray())
	require.ElementsMatch(t, []uint32{1}, v.DocsByIndex(1).ToArray())

	v.Add(2, 1)
	require.ElementsMatch(t, []uint32{1, 2}, v.DocsByIndex(0).ToArray())
	require.ElementsMatch(t, []uint32{1}, v.DocsByIndex(1).ToArray())
}

func Test_docValues_DocsByValue(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	require.ElementsMatch(t, []uint32{1}, v.DocsByValue(1).ToArray())
	v.Add(1, 2)
	require.ElementsMatch(t, []uint32{1}, v.DocsByValue(1).ToArray())
	v.Add(2, 1)
	require.ElementsMatch(t, []uint32{1, 2}, v.DocsByValue(1).ToArray())
}

func Test_docValues_Add(t *testing.T) {
	v := newDocValues[int32]()
	require.Len(t, v.List, 0)
	require.Len(t, v.Values, 0)
	require.Len(t, v.Counters, 0)
	require.Len(t, v.Docs, 0)

	v.Add(1, 1)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)
	require.Len(t, v.Docs, 1)

	v.Add(1, 1)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)
	require.Len(t, v.Docs, 1)

	v.Add(1, 2)
	require.Len(t, v.List, 2)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 2)
	require.Len(t, v.Docs, 2)
}

func Test_docValues_Delete(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	v.Add(2, 2)

	v.DeleteDoc(2)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)
	require.Len(t, v.Docs, 1)

	v.DeleteDoc(1)
	require.Len(t, v.List, 0)
	require.Len(t, v.Values, 0)
	require.Len(t, v.Counters, 0)
	require.Len(t, v.Docs, 0)
}

func Test_docValues_MinValue(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		v := newDocValues[bool]()

		vv, ok := v.MinValue()
		assert.Equal(t, false, vv)
		assert.Equal(t, false, ok)

		v.Add(1, true)

		vv, ok = v.MinValue()
		assert.Equal(t, true, vv)
		assert.Equal(t, true, ok)

		v.Add(2, false)

		vv, ok = v.MinValue()
		assert.Equal(t, false, vv)
		assert.Equal(t, true, ok)
	})
	t.Run("int32", func(t *testing.T) {
		v := newDocValues[int32]()

		vv, ok := v.MinValue()
		assert.Equal(t, int32(0), vv)
		assert.Equal(t, false, ok)

		v.Add(1, 1)

		vv, ok = v.MinValue()
		assert.Equal(t, int32(1), vv)
		assert.Equal(t, true, ok)

		v.Add(2, 100)

		vv, ok = v.MinValue()
		assert.Equal(t, int32(1), vv)
		assert.Equal(t, true, ok)
	})
}

func Test_docValues_MaxValue(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		v := newDocValues[bool]()

		vv, ok := v.MaxValue()
		assert.Equal(t, false, vv)
		assert.Equal(t, false, ok)

		v.Add(1, true)

		vv, ok = v.MaxValue()
		assert.Equal(t, true, vv)
		assert.Equal(t, true, ok)

		v.Add(2, false)

		vv, ok = v.MaxValue()
		assert.Equal(t, true, vv)
		assert.Equal(t, true, ok)
	})
	t.Run("int32", func(t *testing.T) {
		v := newDocValues[int32]()

		vv, ok := v.MaxValue()
		assert.Equal(t, int32(0), vv)
		assert.Equal(t, false, ok)

		v.Add(1, 1)

		vv, ok = v.MaxValue()
		assert.Equal(t, int32(1), vv)
		assert.Equal(t, true, ok)

		v.Add(2, 100)

		vv, ok = v.MaxValue()
		assert.Equal(t, int32(100), vv)
		assert.Equal(t, true, ok)
	})
}

func Test_docValues_FindGt(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 10)
	v.Add(1, 20)
	v.Add(1, 30)

	assert.EqualValues(t, 0, v.FindGt(0))
	assert.EqualValues(t, 1, v.FindGt(10))
	assert.EqualValues(t, 2, v.FindGt(20))
	assert.EqualValues(t, 3, v.FindGt(30))
}

func Test_docValues_FindGte(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 10)
	v.Add(1, 20)
	v.Add(1, 30)

	assert.EqualValues(t, 0, v.FindGte(0))
	assert.EqualValues(t, 0, v.FindGte(10))
	assert.EqualValues(t, 1, v.FindGte(20))
	assert.EqualValues(t, 2, v.FindGte(30))
	assert.EqualValues(t, 3, v.FindGte(40))
}

func Test_docValues_FindLt(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 10)
	v.Add(1, 20)
	v.Add(1, 30)

	assert.EqualValues(t, -1, v.FindLt(10))
	assert.EqualValues(t, 0, v.FindLt(20))
	assert.EqualValues(t, 1, v.FindLt(30))
	assert.EqualValues(t, 2, v.FindLt(40))
}

func Test_docValues_FindLte(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 10)
	v.Add(1, 20)
	v.Add(1, 30)

	assert.EqualValues(t, -1, v.FindLte(0))
	assert.EqualValues(t, 0, v.FindLte(10))
	assert.EqualValues(t, 1, v.FindLte(20))
	assert.EqualValues(t, 2, v.FindLte(30))
	assert.EqualValues(t, 2, v.FindLte(40))
}
