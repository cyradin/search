package schema

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
	t.Run("must return error if extra keys provided", func(t *testing.T) {
		f, err := TokenizerRegexpFunc(map[string]interface{}{"extra": 4})
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("must return error if pattern not provided", func(t *testing.T) {
		f, err := TokenizerRegexpFunc(nil)
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("must return error if non-string pattern provided", func(t *testing.T) {
		f, err := TokenizerRegexpFunc(map[string]interface{}{"pattern": 4})
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("must return error if pattern is not a valid regexp", func(t *testing.T) {
		f, err := TokenizerRegexpFunc(map[string]interface{}{"pattern": "("})
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("empty", func(t *testing.T) {
		var data []string
		f, err := TokenizerRegexpFunc(map[string]interface{}{"pattern": "\\s"})
		require.NoError(t, err)
		result := f(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello  world ",
		}
		f, err := TokenizerRegexpFunc(map[string]interface{}{"pattern": "\\s"})
		require.NoError(t, err)
		result := f(data)
		require.Equal(t, []string{"hello", "world", "hello", "world"}, result)
	})
}
