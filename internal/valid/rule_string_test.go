package valid

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_String(t *testing.T) {
	t.Run("must return error if value is not a string", func(t *testing.T) {
		err := String()(context.Background(), true)
		assert.Error(t, err)

		err = String()(context.Background(), 1)
		assert.Error(t, err)
	})

	t.Run("must not return error if value is a string", func(t *testing.T) {
		err := String()(context.Background(), "qwe")
		require.NoError(t, err)
	})
}
