package field

import (
	"context"
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
			bm, ok := field.data["value_addition"]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})

		t.Run("bool", func(t *testing.T) {
			field := NewText(testAnalyzer, NewScoring())

			field.Add(1, true)
			bm, ok := field.data["true_addition"]

			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.EqualValues(t, 1, bm.GetCardinality())
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("can return union if both values found", func(t *testing.T) {
			scoring := NewScoring()
			f := NewText(testAnalyzer2, scoring)
			f.Add(1, "foo")
			f.Add(2, "bar")

			result := f.Get(context.Background(), "foo bar")
			require.Equal(t, uint64(2), result.Docs().GetCardinality())
			require.True(t, result.Docs().Contains(1))
			require.True(t, result.Docs().Contains(2))
			require.Greater(t, result.Score(1), 0.0)
			require.Greater(t, result.Score(2), 0.0)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		field := NewText(testAnalyzer2, NewScoring())
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		field.Delete(2)
		require.EqualValues(t, 1, field.data["foo"].GetCardinality())
		require.EqualValues(t, 1, field.data["bar"].GetCardinality())
		require.ElementsMatch(t, []string{"foo", "bar"}, field.values[1])
		require.Nil(t, field.values[2])

		field.Delete(1)
		require.Nil(t, field.data["foo"])
		require.Nil(t, field.data["bar"])
		require.Nil(t, field.values[1])
	})

	t.Run("MarshalBinary-UnmarshalBinary", func(t *testing.T) {
		field := NewText(testAnalyzer2, NewScoring())
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		data, err := field.MarshalBinary()
		require.NoError(t, err)

		field2 := NewText(testAnalyzer2, NewScoring())
		err = field2.UnmarshalBinary(data)
		require.NoError(t, err)
		require.True(t, field2.data["foo"].Contains(1))
		require.True(t, field2.data["bar"].Contains(1))
		require.ElementsMatch(t, []string{"foo", "bar"}, field.values[1])
		require.True(t, field2.data["foo"].Contains(2))
		require.ElementsMatch(t, []string{"foo"}, field.values[2])
		require.Equal(t, field.scoring.data, field2.scoring.data)
	})
}
