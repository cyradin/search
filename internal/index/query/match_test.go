package query

import (
	"context"
	"strings"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_MatchQuery_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request field is not defined", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"query": "query"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query is not defined", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"field": "field"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query is empty", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": ""
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query not a stringable value", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": []
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must not return error if request is valid", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": "query"
		}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})
}

func Test_matchQuery_exec(t *testing.T) {
	f1, err := field.New(schema.TypeKeyword)
	require.NoError(t, err)
	f1.Add(1, "value")

	f2, err := field.New(schema.TypeText, field.FieldOpts{
		Analyzer: func(s []string) []string {
			var result []string
			for _, str := range s {
				result = append(result, strings.Fields(str)...)
			}
			return result
		},
		Scoring: field.NewScoring(),
	})
	require.NoError(t, err)
	f2.Add(1, "foo bar")

	ctx := withFields(context.Background(),
		map[string]field.Field{
			"keyword": f1,
			"text":    f2,
		},
	)

	t.Run("must return empty result if field not found", func(t *testing.T) {
		query := new(MatchQuery)
		mustUnmarshal(t, `{
			"field": "field1",
			"query": "value"
		}`, query)

		result, err := query.Exec(ctx)
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return empty result if value not found", func(t *testing.T) {
		t.Run("keyword", func(t *testing.T) {
			query := new(MatchQuery)
			mustUnmarshal(t, `{
				"field": "keyword",
				"query": "value1"
			}`, query)

			result, err := query.Exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})
		t.Run("text", func(t *testing.T) {
			query := new(MatchQuery)
			mustUnmarshal(t, `{
				"field": "text",
				"query": "baz"
			}`, query)

			result, err := query.Exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})
	})

	t.Run("must return non-empty result if value is found", func(t *testing.T) {
		t.Run("keyword", func(t *testing.T) {
			query := new(MatchQuery)
			mustUnmarshal(t, `{
				"field": "keyword",
				"query": "value"
			}`, query)

			result, err := query.Exec(ctx)
			require.NoError(t, err)
			require.False(t, result.Docs().IsEmpty())
			require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
			require.Greater(t, result.Score(1), 0.0)
		})
		t.Run("text", func(t *testing.T) {
			t.Run("two words, one found", func(t *testing.T) {
				query := new(MatchQuery)
				mustUnmarshal(t, `{
					"field": "text",
					"query": "bar baz"
				}`, query)

				result, err := query.Exec(ctx)
				require.NoError(t, err)
				require.False(t, result.Docs().IsEmpty())
				require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
				require.Greater(t, result.Score(1), 0.0)
			})
			t.Run("two words, both found", func(t *testing.T) {
				query := new(MatchQuery)
				mustUnmarshal(t, `{
					"field": "text",
					"query": "foo bar"
				}`, query)

				result, err := query.Exec(ctx)
				require.NoError(t, err)
				require.False(t, result.Docs().IsEmpty())
				require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
				require.Greater(t, result.Score(1), 0.0)
			})
			t.Run("one word", func(t *testing.T) {
				query := new(MatchQuery)
				mustUnmarshal(t, `{
					"field": "text",
					"query": "foo"
				}`, query)

				result, err := query.Exec(ctx)
				require.NoError(t, err)
				require.False(t, result.Docs().IsEmpty())
				require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
				require.Greater(t, result.Score(1), 0.0)
			})
		})
	})
}
