package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_All_AddValue(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		ctx := context.Background()
		field := NewAll(ctx, "")
		values := []testFieldValue{
			{id: 1, value: true},
			{id: 1, value: 1},
		}

		for _, v := range values {
			field.AddValue(v.id, v.value)
			bm, ok := field.inner.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(v.id))
		}

		bm, ok := field.inner.data[true]
		require.True(t, ok)
		require.EqualValues(t, 1, bm.GetCardinality())
	})
}
