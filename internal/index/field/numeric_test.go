package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_numericField_Add(t *testing.T) {
	t.Run("int8", test_numericField_Add[int8])
	t.Run("int16", test_numericField_Add[int16])
	t.Run("int32", test_numericField_Add[int32])
	t.Run("int64", test_numericField_Add[int64])
	t.Run("uint64", test_numericField_Add[uint64])
	t.Run("float32", test_numericField_Add[float32])
	t.Run("float64", test_numericField_Add[float64])
}

func test_numericField_Add[T NumericConstraint](t *testing.T) {
	t.Run("string", func(t *testing.T) {
		field := NewNumeric[T]()
		field.Add(1, "qwe")

		require.EqualValues(t, 0, len(field.data))
	})
	t.Run("numeric", func(t *testing.T) {
		field := NewNumeric[T]()
		field.Add(1, 10)
		field.Add(1, 20)
		field.Add(1, 50)
		field.Add(1, 1)
		field.Add(1, 20)

		field.Add(2, 10)

		require.EqualValues(t, 1, field.data[1].GetCardinality())
		require.EqualValues(t, 2, field.data[10].GetCardinality())
		require.EqualValues(t, 1, field.data[20].GetCardinality())
		require.EqualValues(t, 1, field.data[50].GetCardinality())
		require.True(t, field.data[1].Contains(1))
		require.True(t, field.data[10].Contains(1))
		require.True(t, field.data[20].Contains(1))
		require.True(t, field.data[50].Contains(1))

		require.True(t, field.data[10].Contains(2))
		require.False(t, field.data[1].Contains(2))

		require.EqualValues(t, map[T]struct{}{1: {}, 10: {}, 20: {}, 50: {}}, field.values[1])
		require.EqualValues(t, map[T]struct{}{10: {}}, field.values[2])

		require.EqualValues(t, []T{1, 10, 20, 50}, field.list)
	})
}

func Test_numericField_Term(t *testing.T) {
	t.Run("int8", test_numericField_Term[int8])
	t.Run("int16", test_numericField_Term[int16])
	t.Run("int32", test_numericField_Term[int32])
	t.Run("int64", test_numericField_Term[int64])
	t.Run("uint64", test_numericField_Term[uint64])
	t.Run("float32", test_numericField_Term[float32])
	t.Run("float64", test_numericField_Term[float64])
}

func test_numericField_Term[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 1)

	result := field.Term(context.Background(), 1)
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.Term(context.Background(), 2)
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_numericField_Delete(t *testing.T) {
	t.Run("int8", test_numericField_Delete[int8])
	t.Run("int16", test_numericField_Delete[int16])
	t.Run("int32", test_numericField_Delete[int32])
	t.Run("int64", test_numericField_Delete[int64])
	t.Run("uint64", test_numericField_Delete[uint64])
	t.Run("float32", test_numericField_Delete[float32])
	t.Run("float64", test_numericField_Delete[float64])
}

func test_numericField_Delete[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 50)
	field.Add(1, 2)
	field.Add(1, 20)
	field.Add(2, 1)
	field.Add(2, 10)
	field.Add(2, 30)
	field.Add(2, 60)

	field.Delete(2)
	require.EqualValues(t, 1, field.data[2].GetCardinality())
	require.EqualValues(t, 1, field.data[10].GetCardinality())
	require.EqualValues(t, 1, field.data[20].GetCardinality())
	require.EqualValues(t, 1, field.data[50].GetCardinality())
	require.Nil(t, field.data[1])
	require.Nil(t, field.data[30])
	require.Nil(t, field.data[60])
	require.EqualValues(t, map[T]struct{}{2: {}, 10: {}, 20: {}, 50: {}}, field.values[1])
	require.Nil(t, field.values[2])

	require.ElementsMatch(t, []T{2, 10, 20, 50}, field.list)

	field.Delete(1)
	require.Nil(t, field.data[1])
	require.Nil(t, field.data[10])
	require.Nil(t, field.data[20])
	require.Nil(t, field.data[50])
	require.Nil(t, field.values[1])
}

func Test_numericField_Data(t *testing.T) {
	t.Run("int8", test_numericField_Data[int8])
	t.Run("int16", test_numericField_Data[int16])
	t.Run("int32", test_numericField_Data[int32])
	t.Run("int64", test_numericField_Data[int64])
	t.Run("uint64", test_numericField_Data[uint64])
	t.Run("float32", test_numericField_Data[float32])
	t.Run("float64", test_numericField_Data[float64])
}

func test_numericField_Data[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 1)
	field.Add(1, 2)
	field.Add(2, 1)

	result := field.Data(1)
	require.ElementsMatch(t, []T{T(1), T(2)}, result)
	result = field.Data(2)
	require.ElementsMatch(t, []T{T(1)}, result)
}

func Test_numericField_Marshal(t *testing.T) {
	t.Run("int8", test_numericField_Marshal[int8])
	t.Run("int16", test_numericField_Marshal[int16])
	t.Run("int32", test_numericField_Marshal[int32])
	t.Run("int64", test_numericField_Marshal[int64])
	t.Run("uint64", test_numericField_Marshal[uint64])
	t.Run("float32", test_numericField_Marshal[float32])
	t.Run("float64", test_numericField_Marshal[float64])
}

func test_numericField_Marshal[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 1)
	field.Add(1, 2)
	field.Add(2, 1)

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := NewNumeric[T]()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.data[1].Contains(1))
	require.True(t, field2.data[2].Contains(1))
	require.EqualValues(t, map[T]struct{}{1: {}, 2: {}}, field.values[1])
	require.True(t, field2.data[1].Contains(2))
	require.EqualValues(t, map[T]struct{}{1: {}}, field.values[2])
}

func Test_numericField_findGt(t *testing.T) {
	t.Run("int8", test_numericField_findGt[int8])
	t.Run("int16", test_numericField_findGt[int16])
	t.Run("int32", test_numericField_findGt[int32])
	t.Run("int64", test_numericField_findGt[int64])
	t.Run("uint64", test_numericField_findGt[uint64])
	t.Run("float32", test_numericField_findGt[float32])
	t.Run("float64", test_numericField_findGt[float64])
}

func test_numericField_findGt[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 30)

	assert.EqualValues(t, 1, field.findGt(10))
	assert.EqualValues(t, 2, field.findGt(20))
	assert.EqualValues(t, 3, field.findGt(30))
}

func Test_numericField_findGte(t *testing.T) {
	t.Run("int8", test_numericField_findGte[int8])
	t.Run("int16", test_numericField_findGte[int16])
	t.Run("int32", test_numericField_findGte[int32])
	t.Run("int64", test_numericField_findGte[int64])
	t.Run("uint64", test_numericField_findGte[uint64])
	t.Run("float32", test_numericField_findGte[float32])
	t.Run("float64", test_numericField_findGte[float64])
}

func test_numericField_findGte[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 30)

	assert.EqualValues(t, 0, field.findGte(10))
	assert.EqualValues(t, 1, field.findGte(20))
	assert.EqualValues(t, 2, field.findGte(30))
	assert.EqualValues(t, 3, field.findGte(40))
}

func Test_numericField_findLt(t *testing.T) {
	t.Run("int8", test_numericField_findLt[int8])
	t.Run("int16", test_numericField_findLt[int16])
	t.Run("int32", test_numericField_findLt[int32])
	t.Run("int64", test_numericField_findLt[int64])
	t.Run("uint64", test_numericField_findLt[uint64])
	t.Run("float32", test_numericField_findLt[float32])
	t.Run("float64", test_numericField_findLt[float64])
}

func test_numericField_findLt[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 30)

	assert.EqualValues(t, -1, field.findLt(10))
	assert.EqualValues(t, 0, field.findLt(20))
	assert.EqualValues(t, 1, field.findLt(30))
	assert.EqualValues(t, 2, field.findLt(40))
}

func Test_numericField_findLte(t *testing.T) {
	t.Run("int8", test_numericField_findLte[int8])
	t.Run("int16", test_numericField_findLte[int16])
	t.Run("int32", test_numericField_findLte[int32])
	t.Run("int64", test_numericField_findLte[int64])
	t.Run("uint64", test_numericField_findLte[uint64])
	t.Run("float32", test_numericField_findLte[float32])
	t.Run("float64", test_numericField_findLte[float64])
}

func test_numericField_findLte[T NumericConstraint](t *testing.T) {
	field := NewNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 30)

	assert.EqualValues(t, -1, field.findLte(0))
	assert.EqualValues(t, 0, field.findLte(10))
	assert.EqualValues(t, 1, field.findLte(20))
	assert.EqualValues(t, 2, field.findLte(30))
	assert.EqualValues(t, 2, field.findLte(40))
}
