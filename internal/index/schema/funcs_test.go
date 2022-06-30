package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NopFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := NopFunc()(data)
		require.Equal(t, data, result)
	})
	t.Run("not empty", func(t *testing.T) {
		data := []string{"qwerty", "asdfgh"}
		result := NopFunc()(data)
		require.Equal(t, data, result)
	})
}

func Test_DedupFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := DedupFunc()(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello",
			"hello world",
		}
		result := DedupFunc()(data)
		require.Equal(t, []string{"hello world", "hello"}, result)
	})
}
