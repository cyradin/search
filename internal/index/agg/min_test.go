package agg

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MinAgg_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		v := new(MinAgg)
		mustUnmarshal(t, `{}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must not return error if request is valid", func(t *testing.T) {
		v := new(MinAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		assert.NoError(t, err)
	})
}

func Test_MinAgg_Exec(t *testing.T) {
	t.Run("must return valid agg result", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)

		f, err := field.New(schema.TypeInteger)
		require.NoError(t, err)

		f.Add(1, 1)
		f.Add(2, 1)
		f.Add(2, 2)

		agg := new(MinAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, agg)

		result, err := agg.Exec(context.Background(), Fields{"field": f}, bm)
		require.NoError(t, err)

		require.Equal(t, MinResult{Value: int32(1)}, result)
	})

	t.Run("must return valid agg result with sub-aggs", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)

		f1, err := field.New(schema.TypeInteger)
		require.NoError(t, err)
		f1.Add(1, 1)
		f1.Add(2, 1)
		f1.Add(2, 2)
		f1.Add(3, 1)

		f2, err := field.New(schema.TypeInteger)
		require.NoError(t, err)
		f2.Add(1, 3)
		f2.Add(2, 4)
		f2.Add(3, 5)

		agg := new(MinAgg)
		mustUnmarshal(t, `{
				"field": "field",
				"aggs": {
					"agg":	{
						"type": "terms",
						"field": "field2",
						"size": 10
					}
				}
			}`, agg)

		result, err := agg.Exec(context.Background(), Fields{"field": f1, "field2": f2}, bm)
		require.NoError(t, err)

		require.Equal(t, MinResult{
			Value: int32(1),
			Aggs: map[string]interface{}{
				"agg": TermsResult{
					Buckets: []TermsBucket{
						{Key: int32(3), DocCount: 1},
						{Key: int32(4), DocCount: 1},
					},
				},
			},
		}, result)
	})
}
