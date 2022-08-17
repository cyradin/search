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

func Test_TermsAgg_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		v := new(TermsAgg)
		mustUnmarshal(t, `{}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'size' is < 0", func(t *testing.T) {
		v := new(TermsAgg)
		mustUnmarshal(t, `{
			"field": "field",
			"size": -1
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'field' is empty", func(t *testing.T) {
		v := new(TermsAgg)
		mustUnmarshal(t, `{
			"size": 10
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must not return error if request is valid", func(t *testing.T) {
		v := new(TermsAgg)
		mustUnmarshal(t, `{
			"field": "field",
			"size": 10
		}`, v)

		err := validation.Validate(v)
		assert.NoError(t, err)

		v = new(TermsAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err = validation.Validate(v)
		assert.NoError(t, err)
	})
}

func Test_TermsAgg_Exec(t *testing.T) {
	t.Run("must return valid agg result", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)

		f, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)

		f.Add(1, "foo")
		f.Add(2, "foo")
		f.Add(2, "bar")

		agg := new(TermsAgg)
		mustUnmarshal(t, `{
			"field": "field",
			"size": 10
		}`, agg)

		result, err := agg.Exec(context.Background(), Fields{"field": f}, bm)
		require.NoError(t, err)

		require.Equal(t, TermsResult{Buckets: []TermsBucket{
			{DocCount: 2, Key: "foo"},
			{DocCount: 1, Key: "bar"},
		}}, result)
	})

	t.Run("must return valid agg result with sub-aggs", func(t *testing.T) {
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

		agg := new(TermsAgg)
		mustUnmarshal(t, `{
				"field": "field",
				"size": 10,
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

		require.Equal(t, TermsResult{Buckets: []TermsBucket{
			{
				DocCount: 2, Key: "foo", Aggs: map[string]interface{}{
					"agg": TermsResult{
						Buckets: []TermsBucket{
							{Key: "asdfgh", DocCount: 1},
							{Key: "qwerty", DocCount: 1},
						},
					},
				},
			},
			{
				DocCount: 1, Key: "bar", Aggs: map[string]interface{}{
					"agg": TermsResult{
						Buckets: []TermsBucket{
							{Key: "asdfgh", DocCount: 1},
						},
					},
				},
			},
		}}, result)
	})
}
