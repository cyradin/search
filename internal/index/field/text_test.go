package field

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var testAnalyzer = func(s []string) []string {
	result := make([]string, 0, len(s))
	for _, ss := range s {
		result = append(result, ss+"_addition")
	}
	return result
}

var testAnalyzer2 = func(s []string) []string {
	result := make([]string, 0, len(s))
	for _, ss := range s {
		result = append(result, strings.Split(ss, " ")...)
	}
	return result
}

func Test_Text(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		t.Run("string", func(t *testing.T) {
			value := "value"
			field := NewText(testAnalyzer, NewScoring())

			field.Add(1, value)
			bm, ok := field.inner.data["value_addition"]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})

		t.Run("bool", func(t *testing.T) {
			field := NewText(testAnalyzer, NewScoring())

			field.Add(1, true)
			bm, ok := field.inner.data["true_addition"]

			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})
	})
	t.Run("GetOrAnalyzed", func(t *testing.T) {
		t.Run("can return union if both values found", func(t *testing.T) {
			f := NewText(testAnalyzer2, NewScoring())
			f.Add(1, "foo")
			f.Add(2, "bar")

			result := f.GetOrAnalyzed("foo bar")
			require.Equal(t, uint64(2), result.GetCardinality())
			require.True(t, result.Contains(1))
			require.True(t, result.Contains(2))
		})
	})
	t.Run("GetAndAnalyzed", func(t *testing.T) {
		t.Run("can return intersection if both values found", func(t *testing.T) {
			f := NewText(testAnalyzer2, NewScoring())
			f.Add(1, "foo")
			f.Add(1, "bar")
			f.Add(2, "bar")

			result := f.GetAndAnalyzed("foo bar")
			require.Equal(t, uint64(1), result.GetCardinality())
			require.True(t, result.Contains(1))
		})
	})
}
