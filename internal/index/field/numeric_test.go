package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_numericField(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		t.Run("1", func(t *testing.T) {
			var value int64 = 1
			field := newNumericField[int64]()

			field.Add(1, value)
			bm, ok := field.data[value]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})

		t.Run("string", func(t *testing.T) {
			field := newNumericField[int64]()

			field.Add(1, "qwe")
			_, ok := field.data[0]
			require.False(t, ok)
		})
	})
}
