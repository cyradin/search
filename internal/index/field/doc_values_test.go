package field

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func Test_docValues_ValueByIndex(t *testing.T) {
	v := newDocValues[int32]()

	v.Add(1, 1)
	require.EqualValues(t, 1, v.ValueByIndex(0))

	v.Add(1, 3)
	require.EqualValues(t, 1, v.ValueByIndex(0))
	require.EqualValues(t, 3, v.ValueByIndex(1))

	v.Add(1, 2)
	require.EqualValues(t, 1, v.ValueByIndex(0))
	require.EqualValues(t, 2, v.ValueByIndex(1))
	require.EqualValues(t, 3, v.ValueByIndex(2))
}

func Test_docValues_ValuesByDoc(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	require.ElementsMatch(t, []int32{1}, v.ValuesByDoc(1))
	v.Add(1, 2)
	require.ElementsMatch(t, []int32{1, 2}, v.ValuesByDoc(1))
}

func Test_docValues_Add(t *testing.T) {
	v := newDocValues[int32]()
	require.Len(t, v.List, 0)
	require.Len(t, v.Values, 0)
	require.Len(t, v.Counters, 0)

	v.Add(1, 1)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)

	v.Add(1, 1)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)

	v.Add(1, 2)
	require.Len(t, v.List, 2)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 2)
}

func Test_docValues_Delete(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	v.Add(2, 2)

	v.DeleteDoc(2)
	require.Len(t, v.List, 1)
	require.Len(t, v.Values, 1)
	require.Len(t, v.Counters, 1)

	v.DeleteDoc(1)
	require.Len(t, v.List, 0)
	require.Len(t, v.Values, 0)
	require.Len(t, v.Counters, 0)
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
