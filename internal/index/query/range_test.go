package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_newRangeQuery(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := "{}"
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newRangeQuery(context.Background(), req)
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

		q, err := newRangeQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must return error if request field is empty", func(t *testing.T) {
		query := `{
				"field1": {}
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newRangeQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must return error if request field contains extra keys", func(t *testing.T) {
		query := `{
				"field1": {
					"from": 1,
					"qwerty": 1
				}
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newRangeQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must not return error if request is a valid query", func(t *testing.T) {
		query := `{
				"field1": {
					"from": 1,
					"includeLower": true,
					"to": 100,
					"includeUpper": true
				}
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newRangeQuery(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, q)
	})
}

func Test_rangeQuery_exec(t *testing.T) {
	f, err := field.New(schema.TypeInteger)
	require.NoError(t, err)
	f.Add(1, 1)
	f.Add(2, 2)
	f.Add(3, 3)
	f.Add(4, 4)
	f.Add(5, 5)
	ctx := withFields(context.Background(),
		map[string]field.Field{
			"field": f,
		},
	)

	t.Run("must return empty result if field not found", func(t *testing.T) {
		query := `{
				"field1": {
					"from": 2
				}
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newRangeQuery(ctx, req)
		require.NoError(t, err)

		result, err := q.exec(ctx)
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return valid result", func(t *testing.T) {
		t.Run("[1, 3]", func(t *testing.T) {
			query := `{
				"field": {
					"from": 1,
					"to": 3,
					"includeLower": true,
					"includeUpper": true
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newRangeQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
		})

		t.Run("(1, 4)", func(t *testing.T) {
			query := `{
				"field": {
					"from": 1,
					"to": 4
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newRangeQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []uint32{2, 3}, result.Docs().ToArray())
		})
	})
}
