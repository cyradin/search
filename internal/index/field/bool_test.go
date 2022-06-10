package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Bool_AddValue(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		ctx := context.Background()
		field := NewBool(ctx, "")

		field.AddValue(1, true)
		bm, ok := field.inner.data[true]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})

	t.Run("string", func(t *testing.T) {
		ctx := context.Background()
		field := NewBool(ctx, "")

		field.AddValue(1, "qwe")
		_, ok := field.inner.data[false]
		require.False(t, ok)
	})
}
