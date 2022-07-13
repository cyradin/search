package query

import (
	"context"
	"strings"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_matchQuery(t *testing.T) {
	t.Run("newMatchQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request contains multiple keys", func(t *testing.T) {
			query := `{
				"field1": {},
				"field2": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field contains extra keys", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello",
					"qwerty": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request is a valid query", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})

	t.Run("exec", func(t *testing.T) {
		f1 := field.NewKeyword()
		f1.Add(1, "value")

		f2 := field.NewText(func(s []string) []string {
			var result []string
			for _, str := range s {
				result = append(result, strings.Fields(str)...)
			}
			return result
		}, field.NewScoring())
		f2.Add(1, "foo bar")

		ctx := withFields(context.Background(),
			map[string]field.Field{
				"keyword": f1,
				"text":    f2,
			},
		)

		t.Run("must return empty result if field not found", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "value1"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newMatchQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.bm.IsEmpty())
			require.NotNil(t, result.scores)
			require.Empty(t, result.scores)
		})

		t.Run("must return empty result if value not found", func(t *testing.T) {
			t.Run("keyword", func(t *testing.T) {
				query := `{
					"field": {
						"query": "value1"
					}
				}`
				req, err := decodeQuery(query)
				require.NoError(t, err)

				tq, err := newMatchQuery(ctx, req)
				require.NoError(t, err)

				result, err := tq.exec(ctx)
				require.NoError(t, err)
				require.True(t, result.bm.IsEmpty())
				require.NotNil(t, result.scores)
				require.Empty(t, result.scores)
			})
			t.Run("text", func(t *testing.T) {
				query := `{
					"text": {
						"query": "baz"
					}
				}`
				req, err := decodeQuery(query)
				require.NoError(t, err)

				tq, err := newMatchQuery(ctx, req)
				require.NoError(t, err)

				result, err := tq.exec(ctx)
				require.NoError(t, err)
				require.True(t, result.bm.IsEmpty())
				require.NotNil(t, result.scores)
				require.Empty(t, result.scores)
			})
		})

		t.Run("must return non-empty result if value is found", func(t *testing.T) {
			t.Run("keyword", func(t *testing.T) {
				query := `{
					"keyword": {
						"query": "value"
					}
				}`
				req, err := decodeQuery(query)
				require.NoError(t, err)

				tq, err := newMatchQuery(ctx, req)
				require.NoError(t, err)

				result, err := tq.exec(ctx)
				require.NoError(t, err)
				require.False(t, result.bm.IsEmpty())
				require.ElementsMatch(t, []uint32{1}, result.bm.ToArray())
				require.NotNil(t, result.scores)
				require.Empty(t, result.scores)
			})
			t.Run("text", func(t *testing.T) {
				t.Run("two words, one found", func(t *testing.T) {
					query := `{
						"text": {
							"query": "bar baz"
						}
					}`
					req, err := decodeQuery(query)
					require.NoError(t, err)

					tq, err := newMatchQuery(ctx, req)
					require.NoError(t, err)

					result, err := tq.exec(ctx)
					require.NoError(t, err)
					require.False(t, result.bm.IsEmpty())
					require.ElementsMatch(t, []uint32{1}, result.bm.ToArray())
					require.NotNil(t, result.scores)
					require.Empty(t, result.scores)
				})
				t.Run("two words, both found", func(t *testing.T) {
					query := `{
						"text": {
							"query": "foo bar"
						}
					}`
					req, err := decodeQuery(query)
					require.NoError(t, err)

					tq, err := newMatchQuery(ctx, req)
					require.NoError(t, err)

					result, err := tq.exec(ctx)
					require.NoError(t, err)
					require.False(t, result.bm.IsEmpty())
					require.ElementsMatch(t, []uint32{1}, result.bm.ToArray())
					require.NotNil(t, result.scores)
					require.Empty(t, result.scores)
				})
				t.Run("one word", func(t *testing.T) {
					query := `{
						"text": {
							"query": "foo"
						}
					}`
					req, err := decodeQuery(query)
					require.NoError(t, err)

					tq, err := newMatchQuery(ctx, req)
					require.NoError(t, err)

					result, err := tq.exec(ctx)
					require.NoError(t, err)
					require.False(t, result.bm.IsEmpty())
					require.ElementsMatch(t, []uint32{1}, result.bm.ToArray())
					require.NotNil(t, result.scores)
					require.Empty(t, result.scores)
				})
			})
		})
	})
}
