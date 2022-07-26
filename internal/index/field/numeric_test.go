package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_numericField(t *testing.T) {
	test_numericField[int8](t)
	test_numericField[int16](t)
	test_numericField[int32](t)
	test_numericField[int64](t)
	test_numericField[uint64](t)
	test_numericField[float32](t)
	test_numericField[float64](t)
}

func test_numericField[T NumericConstraint](t *testing.T) {
	t.Run("Add", func(t *testing.T) {
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
		})
	})

	t.Run("Term", func(t *testing.T) {
		field := NewNumeric[T]()
		field.Add(1, 1)

		result := field.Term(context.Background(), 1)
		require.True(t, result.Docs().Contains(1))
		require.EqualValues(t, 1, result.Docs().GetCardinality())

		result = field.Term(context.Background(), 2)
		require.False(t, result.Docs().Contains(1))
		require.EqualValues(t, 0, result.Docs().GetCardinality())
	})

	t.Run("Delete", func(t *testing.T) {
		field := NewNumeric[T]()
		field.Add(1, 1)
		field.Add(1, 2)
		field.Add(2, 1)

		field.Delete(2)
		require.EqualValues(t, 1, field.data[1].GetCardinality())
		require.EqualValues(t, 1, field.data[2].GetCardinality())

		require.EqualValues(t, map[T]struct{}{1: {}, 2: {}}, field.values[1])
		require.Nil(t, field.values[2])

		field.Delete(1)
		require.Nil(t, field.data[1])
		require.Nil(t, field.data[2])
		require.Nil(t, field.values[1])
	})

	t.Run("Data", func(t *testing.T) {
		field := NewNumeric[T]()
		field.Add(1, 1)
		field.Add(1, 2)
		field.Add(2, 1)

		result := field.Data(1)
		require.ElementsMatch(t, []T{T(1), T(2)}, result)
		result = field.Data(2)
		require.ElementsMatch(t, []T{T(1)}, result)
	})

	t.Run("MarshalBinary-UnmarshalBinary", func(t *testing.T) {
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
	})
}
