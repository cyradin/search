package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Keyword_Add(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		value := "qwe"
		field := NewKeyword()

		field.Add(1, value)
		bm, ok := field.inner.data[value]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})

	t.Run("bool", func(t *testing.T) {
		field := NewKeyword()

		field.Add(1, true)
		bm, ok := field.inner.data["true"]

		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})
}
