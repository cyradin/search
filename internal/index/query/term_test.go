package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_termQuery(t *testing.T) {
	t.Run("newTermQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
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

			q, err := newTermQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
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

			q, err := newTermQuery(context.Background(), req)
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

			q, err := newTermQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})

	t.Run("exec", func(t *testing.T) {
		f := field.NewKeyword()
		f.Add(1, "value")
		ctx := withFields(context.Background(),
			map[string]field.Field{
				"field": f,
			},
		)

		t.Run("must return empty result if field not found", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "value"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})

		t.Run("must return empty result if value not found", func(t *testing.T) {
			query := `{
				"field": {
					"query": "value1"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})

		t.Run("must return non-empty result if value is found", func(t *testing.T) {
			query := `{
				"field": {
					"query": "value"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)

			require.False(t, result.Docs().IsEmpty())
			require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
		})
	})
}

func Test_termsQuery(t *testing.T) {
	t.Run("newTermsQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermsQuery(context.Background(), req)
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

			q, err := newTermsQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermsQuery(context.Background(), req)
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

			q, err := newTermsQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request query is not an array", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermsQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request query is a valid query", func(t *testing.T) {
			query := `{
				"field1": {
					"query": ["hello"]
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermsQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})

	t.Run("exec", func(t *testing.T) {
		f := field.NewKeyword()
		f.Add(1, "value")
		ctx := withFields(context.Background(),
			map[string]field.Field{
				"field": f,
			},
		)

		t.Run("must return empty result if field not found", func(t *testing.T) {
			query := `{
				"field1": {
					"query": ["value1"]
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermsQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})

		t.Run("must return empty result if value not found", func(t *testing.T) {
			query := `{
				"field": {
					"query": ["value1"]
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermsQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.Docs().IsEmpty())
		})

		t.Run("must return non-empty result if value is found", func(t *testing.T) {
			query := `{
				"field": {
					"query": ["value"]
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermsQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.False(t, result.Docs().IsEmpty())
			require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
		})
	})
}
