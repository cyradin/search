package agg

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_RangeAgg_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' not defined", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' are empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' 'key' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"from": 10,
					"to": 50
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' 'from' and 'to' are empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key"
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must not return error if request 'ranges' 'from' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key",
					"to": 50
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.NoError(t, err)
	})

	t.Run("must not return error if request 'ranges' 'to' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key",
					"from": 10
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.NoError(t, err)
	})
}

func Test_RangeAgg_Exec(t *testing.T) {
	f1, err := field.New(schema.TypeInteger)
	require.NoError(t, err)

	f1.Add(1, 1)
	f1.Add(1, 2)
	f1.Add(1, 3)

	f1.Add(2, 1)
	f1.Add(2, 2)
	f1.Add(2, 4)

	f1.Add(3, 3)
	f1.Add(3, 4)

	bm := roaring.New()
	bm.Add(1)
	bm.Add(2)
	bm.Add(3)
	bm.Add(4)

	ctx := withFields(context.Background(),
		map[string]field.Field{
			"field": f1,
		},
	)

	t.Run("must return correct results", func(t *testing.T) {
		agg := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "[1..",
					"from": 1
				},
				{
					"key": "[1..2]",
					"from": 1,
					"to": 2
				},
				{
					"key": "[2..3]",
					"from": 2,
					"to": 3
				},
				{
					"key": "..4]",
					"to": 4
				}
			],
			"field": "field"
		}`, agg)

		result, err := agg.Exec(ctx, bm)
		require.NoError(t, err)

		require.Equal(t, RangeResult{Buckets: []RangeBucket{
			{Key: "[1..", From: int32(1), To: nil, DocCount: 3},
			{Key: "[1..2]", From: int32(1), To: int32(2), DocCount: 2},
			{Key: "[2..3]", From: int32(2), To: int32(3), DocCount: 3},
			{Key: "..4]", To: int32(4), DocCount: 3},
		}}, result)
	})

	t.Run("must return correct results for subaggregations", func(t *testing.T) {
		agg := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "[1..2]",
					"from": 1,
					"to": 2
				}
			],
			"field": "field",
			"aggs": {
				"agg": {
					"type": "terms",
					"field": "field",
					"size": 10
				}
			}
		}`, agg)

		result, err := agg.Exec(ctx, bm)
		require.NoError(t, err)

		require.Equal(t, RangeResult{Buckets: []RangeBucket{
			{Key: "[1..2]", From: int32(1), To: int32(2), DocCount: 2, Aggs: map[string]interface{}{
				"agg": TermsResult{
					Buckets: []TermsBucket{
						{Key: int32(1), DocCount: 2},
						{Key: int32(2), DocCount: 2},
						// @todo do not return values outside range in subaggs
						{Key: int32(3), DocCount: 1},
						{Key: int32(4), DocCount: 1},
					},
				},
			}},
		}}, result)
	})
}
