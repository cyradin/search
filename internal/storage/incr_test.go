package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Increment(t *testing.T) {
	ctx := testContext(t)
	t.Run("must return 1 if value not existed before", func(t *testing.T) {
		result, err := Increment(ctx, "key")
		require.NoError(t, err)
		require.Equal(t, int64(1), result)
	})

	t.Run("must return value + 1", func(t *testing.T) {
		result, err := Increment(ctx, "key")
		require.NoError(t, err)
		require.Equal(t, int64(2), result)
	})
}

func Test_IncrementBy(t *testing.T) {
	ctx := testContext(t)
	t.Run("must return N if value not existed before", func(t *testing.T) {
		result, err := IncrementBy(ctx, "key", 100)
		require.NoError(t, err)
		require.Equal(t, int64(100), result)
	})

	t.Run("must return value + N", func(t *testing.T) {
		result, err := IncrementBy(ctx, "key", 20)
		require.NoError(t, err)
		require.Equal(t, int64(120), result)
	})
}
