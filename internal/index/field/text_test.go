package field

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testAnalyzer Analyzer = func(s []string) []string {
	result := make([]string, 0, len(s))
	for _, ss := range s {
		result = append(result, ss+"_addition")
	}
	return result
}

func Test_Text_AddValue(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		value := "value"
		field := NewText(testAnalyzer)

		field.AddValue(1, value)
		bm, ok := field.inner.data["value_addition"]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})

	t.Run("bool", func(t *testing.T) {
		field := NewText(testAnalyzer)

		field.AddValue(1, true)
		bm, ok := field.inner.data["true_addition"]

		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})
}
