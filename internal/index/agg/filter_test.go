package agg

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func Test_FilterAgg_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		v := new(FilterAgg)
		err := jsoniter.Unmarshal([]byte(`{}`), v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'filter' is not an object", func(t *testing.T) {
		v := new(FilterAgg)

		err := jsoniter.Unmarshal([]byte(`{
			"filter": "1"
		}`), v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'filter' is not a valid query", func(t *testing.T) {
		v := new(FilterAgg)

		err := jsoniter.Unmarshal([]byte(`{
			"filter": {"type": "terms"}
		}`), v)
		require.Error(t, err)
	})

	t.Run("must not return error if request 'filter' is a valid query", func(t *testing.T) {
		v := new(FilterAgg)

		err := jsoniter.Unmarshal([]byte(`{
			"filter": {"type": "terms", "field": "field", "query": [1,2,3]}
		}`), v)
		require.NoError(t, err)

		err = validation.Validate(v)
		require.NoError(t, err)
	})
}

func Test_FilterAgg_Exec(t *testing.T) {
	t.Run("must return valid agg result", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)
		bm.Add(3)

		f, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)

		f.Add(1, "foo")
		f.Add(2, "foo")
		f.Add(2, "bar")
		f.Add(3, "foo")
		f.Add(4, "foo")

		agg := new(FilterAgg)
		mustUnmarshal(t, `{
			"filter": {
				"type": "terms",
				"field": "field",
				"query": ["foo"]
			}
		}`, agg)

		result, err := agg.Exec(context.Background(), Fields{"field": f}, bm)
		require.NoError(t, err)

		require.Equal(t, FilterResult{DocCount: 3}, result)
	})

	t.Run("must return valid agg result with sub-aggs", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)
		bm.Add(3)

		f, err := field.New(schema.TypeKeyword)
		require.NoError(t, err)

		f.Add(1, "foo")
		f.Add(2, "foo")
		f.Add(2, "bar")
		f.Add(3, "foo")
		f.Add(4, "foo")

		agg := new(FilterAgg)
		mustUnmarshal(t, `{
			"filter": {
				"type": "terms",
				"field": "field",
				"query": ["foo"]
			},
			"aggs": {
				"sub": {
					"type": "terms",
					"field": "field",
					"size": 10
				}
			}
		}`, agg)

		result, err := agg.Exec(context.Background(), Fields{"field": f}, bm)
		require.NoError(t, err)

		require.Equal(t, FilterResult{DocCount: 3, Aggs: map[string]interface{}{
			"sub": TermsResult{
				Buckets: []TermsBucket{
					{Key: "foo", DocCount: 3},
					{Key: "bar", DocCount: 1},
				},
			},
		}}, result)
	})
}
