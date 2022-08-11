package agg

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_newTermAgg(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := `{}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request does not contain 'size' key", func(t *testing.T) {
		query := `{
			"field": "field"
		}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request size key is < 0", func(t *testing.T) {
		query := `{
				"field": "field",
				"size": -1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request does not contain 'field' key", func(t *testing.T) {
		query := `{
				"size": 1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request 'field' value is not a string", func(t *testing.T) {
		query := `{
				"field": true,
				"size": 1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request 'field' value is empty", func(t *testing.T) {
		query := `{
				"field": "",
				"size": 1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must not return error if request is a valid term aggregation", func(t *testing.T) {
		query := `{
				"field": "field",
				"size": 1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.NoError(t, err)
		require.NotNil(t, q)
	})
}

func Test_termsAgg_exec(t *testing.T) {
	t.Run("must return valid agg result", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)

		f, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)

		f.Add(1, "foo")
		f.Add(2, "foo")
		f.Add(2, "bar")
		ctx := withFields(context.Background(),
			map[string]field.Field{
				"field": f,
			},
		)

		query := `{
			"field": "field",
			"size": 10
		}`
		req := mustDecodeRequest(t, query)

		agg, err := newTermsAgg(ctx, req, nil)
		require.NoError(t, err)
		require.NotNil(t, agg)

		result, err := agg.exec(ctx, bm)
		require.NoError(t, err)

		require.Equal(t, Terms{Buckets: []TermsBucket{
			{DocCount: 2, Key: "foo"},
			{DocCount: 1, Key: "bar"},
		}}, result)
	})

	t.Run("must return valid agg result with subAggs", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)

		f1, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)
		f1.Add(1, "foo")
		f1.Add(2, "foo")
		f1.Add(2, "bar")

		f2, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)
		f2.Add(1, "qwerty")
		f2.Add(2, "asdfgh")

		ctx := withFields(context.Background(),
			map[string]field.Field{
				"field":  f1,
				"field2": f2,
			},
		)

		query := `{
			"field": "field",
			"size": 10
		}`
		req := mustDecodeRequest(t, query)

		subAggs := `{
			"agg":	{
				"terms": {
					"field": "field2",
					"size": 10
				}
			}
		}`
		subAggsReq := mustDecodeRequest(t, subAggs)

		agg, err := newTermsAgg(ctx, req, subAggsReq)
		require.NoError(t, err)
		require.NotNil(t, agg)

		result, err := agg.exec(ctx, bm)
		require.NoError(t, err)

		require.Equal(t, Terms{Buckets: []TermsBucket{
			{
				DocCount: 2, Key: "foo", SubAggs: map[string]interface{}{
					"agg": Terms{
						Buckets: []TermsBucket{
							{Key: "asdfgh", DocCount: 1},
							{Key: "qwerty", DocCount: 1},
						},
					},
				},
			},
			{
				DocCount: 1, Key: "bar", SubAggs: map[string]interface{}{
					"agg": Terms{
						Buckets: []TermsBucket{
							{Key: "asdfgh", DocCount: 1},
						},
					},
				},
			},
		}}, result)
	})
}
