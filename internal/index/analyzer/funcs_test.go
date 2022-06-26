package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testNopFunc = func(s []string) []string {
	return s
}

func Test_NopFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := NopFunc(testNopFunc)(data)
		require.Equal(t, data, result)
	})
	t.Run("not empty", func(t *testing.T) {
		data := []string{"qwerty", "asdfgh"}
		result := NopFunc(testNopFunc)(data)
		require.Equal(t, data, result)
	})
}

func Test_TokenizeWhitespace(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := WhitespaceTokenizerFunc(testNopFunc)(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello  world ",
		}
		result := WhitespaceTokenizerFunc(testNopFunc)(data)
		require.Equal(t, []string{"hello", "world", "hello", "world"}, result)
	})
}

func Test_DedupFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := DedupFunc(testNopFunc)(data)
		require.Equal(t, data, result)
	})

	t.Run("not empty", func(t *testing.T) {
		data := []string{
			"hello world",
			"hello",
			"hello world",
		}
		result := DedupFunc(testNopFunc)(data)
		require.Equal(t, []string{"hello world", "hello"}, result)
	})
}
