package field

import (
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
		t.Run("1", func(t *testing.T) {
			var value T = 1
			field := NewNumeric[T]()

			field.Add(1, value)
			bm, ok := field.data[value]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})

		t.Run("string", func(t *testing.T) {
			field := NewNumeric[T]()

			field.Add(1, "qwe")
			_, ok := field.data[0]
			require.False(t, ok)
		})
	})
}
