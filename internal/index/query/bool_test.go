package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_newBoolQuery(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		shouldCnt int
		mustCnt   int
		filterCnt int
	}{
		{
			name:      "empty_query_return_all",
			query:     `{}`,
			erroneous: false,
		},
		{
			name: "error_array_required",
			query: `
			{
				"should": {
					"term": {
						"field": true
					}
				}
			}
			`,
			erroneous: true,
		},
		{
			name: "error_unknown_bool_query_type",
			query: `
			{
				"invalid": [
					{
						"term": {
							"field": true
						}
					}
				]
			}
			`,
			erroneous: true,
		},
		{
			name: "ok",
			query: `
			{
				"should": [
					{
						"term": {
							"field": true
						}
					},
					{
						"term": {
							"field": false
						}
					}
				],
				"must": [
					{
						"term": {
							"field": true
						}
					}
				],
				"filter": [
					{
						"term": {
							"field": true
						}
					}
				]
			}
			`,
			erroneous: false,
			shouldCnt: 2,
			mustCnt:   1,
			filterCnt: 1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			query, err := decodeQuery(d.query)
			require.NoError(t, err)

			bq, err := newBoolQuery(queryParams{
				data: query,
			})
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.Len(t, bq.should, d.shouldCnt)
			require.Len(t, bq.filter, d.filterCnt)
			require.Len(t, bq.must, d.mustCnt)
		})
	}
}

func Test_boolQuery_exec(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []uint32
	}{
		{
			name:      "empty_query_return_all",
			query:     `{}`,
			erroneous: false,
			expected:  []uint32{1, 2},
		},
		{
			name: "ok_bool_should",
			query: `
			{
				"should": [
					{
						"term": {
							"field": true
						}
					},
					{
						"term": {
							"field": false
						}
					}
				]
			}
			`,
			erroneous: false,
			expected:  []uint32{1, 2},
		},
		{
			name: "ok_bool_must",
			query: `
			{
				"must": [
					{
						"term": {
							"field": true
						}
					},
					{
						"term": {
							"field": false
						}
					}
				]
			}
			`,
			erroneous: false,
			expected:  []uint32{},
		},
		{
			name: "ok_bool_filter",
			query: `
			{
				"filter": [
					{
						"term": {
							"field": true
						}
					},
					{
						"term": {
							"field": true
						}
					}
				]
			}
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1 := field.NewBool(context.Background(), "")
			f1.AddValue(1, true)
			f1.AddValue(2, false)

			f2 := field.NewAll(context.Background(), "")
			f2.AddValue(1, true)
			f2.AddValue(2, false)

			data, err := decodeQuery(d.query)
			require.NoError(t, err)
			require.NoError(t, err)

			bq, err := newBoolQuery(queryParams{
				data:   data,
				fields: map[string]field.Field{"field": f1, field.AllField: f2},
			})
			require.NoError(t, err)

			result, err := bq.exec()
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}

			require.NoError(t, err)

			vals := make([]uint32, 0)
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}
