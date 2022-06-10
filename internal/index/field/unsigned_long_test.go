package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_UnsignedLong_AddValue(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		var value uint64 = 1
		ctx := context.Background()
		field := NewUnsignedLong(ctx, "")

		field.AddValue(1, value)
		bm, ok := field.inner.data[value]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})

	t.Run("string", func(t *testing.T) {
		ctx := context.Background()
		field := NewUnsignedLong(ctx, "")

		field.AddValue(1, "qwe")
		_, ok := field.inner.data[0]
		require.False(t, ok)
	})
}
