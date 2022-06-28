package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_TokenizerWhitespaceFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := TokenizerWhitespaceFunc()(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello  world ",
		}
		result := TokenizerWhitespaceFunc()(data)
		require.Equal(t, []string{"hello", "world", "hello", "world"}, result)
	})
}

func Test_TokenizerRegexpFunc(t *testing.T) {
	t.Run("must return error if expression is invalid", func(t *testing.T) {
		f, err := TokenizerRegexpFunc("(")
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("empty", func(t *testing.T) {
		var data []string
		f, err := TokenizerRegexpFunc("\\s")
		require.NoError(t, err)
		result := f(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello  world ",
		}
		f, err := TokenizerRegexpFunc("\\s")
		require.NoError(t, err)
		result := f(data)
		require.Equal(t, []string{"hello", "world", "hello", "world"}, result)
	})
}
