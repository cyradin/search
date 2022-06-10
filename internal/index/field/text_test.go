package field

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

var testAnalyzer1 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		splitter := regexp.MustCompile(`\s`)
		for _, ss := range s {
			result = append(result, splitter.Split(ss, -1)...)
		}
		return next(result)
	}
}

var testAnalyzer2 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		for _, ss := range s {
			result = append(result, ss+"_addition1")
		}
		return next(result)
	}
}

var testAnalyzer3 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		for _, ss := range s {
			result = append(result, ss+"_addition2")
		}
		return next(result)
	}
}

func Test_Text_AddValue(t *testing.T) {

	t.Run("string", func(t *testing.T) {
		value := "value value1"
		ctx := context.Background()
		field := NewText(ctx, "", testAnalyzer1, testAnalyzer2, testAnalyzer3)

		field.AddValue(1, value)
		bm, ok := field.inner.data["value_addition1_addition2"]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())

		bm, ok = field.inner.data["value1_addition1_addition2"]
		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})

	t.Run("bool", func(t *testing.T) {
		ctx := context.Background()
		field := NewKeyword(ctx, "")

		field.AddValue(1, true)
		bm, ok := field.inner.data["true"]

		require.True(t, ok)
		require.True(t, bm.Contains(1))
		require.EqualValues(t, 1, bm.GetCardinality())
	})
}
