package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_execBool(t *testing.T) {
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
			f1, err := field.NewBool(context.Background(), "")
			require.Nil(t, err)
			err = f1.AddValueSync(1, true)
			require.Nil(t, err)
			err = f1.AddValueSync(2, false)
			require.Nil(t, err)

			f2, err := field.NewAll(context.Background(), "")
			require.Nil(t, err)
			err = f2.AddValueSync(1, true)
			require.Nil(t, err)
			err = f2.AddValueSync(2, false)
			require.Nil(t, err)

			query, err := decodeQuery(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := execBool(query, map[string]field.Field{"field": f1, field.AllField: f2}, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)

			vals := make([]uint32, 0)
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}

func Test_execBoolShould(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []uint32
	}{
		{
			name:      "empty_query",
			query:     `[]`,
			erroneous: false,
			expected:  []uint32{},
		},
		{
			name: "ok_same_values",
			query: `
			[
				{
					"term": {
						"field": "1"
					}
				},
				{
					"term": {
						"field": "1"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name: "ok_union",
			query: `
			[
				{
					"term": {
						"field": "1"
					}
				},
				{
					"term": {
						"field": "2"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1, 2},
		},
		{
			name: "ok_nothing_found",
			query: `
			[
				{
					"term": {
						"field": "3"
					}
				},
				{
					"term": {
						"field": "4"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1, err := field.NewKeyword(context.Background(), "")
			require.Nil(t, err)
			err = f1.AddValueSync(1, "1")
			require.Nil(t, err)
			err = f1.AddValueSync(2, "2")
			require.Nil(t, err)

			query, err := decodeQuerySlice(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := execBoolShould(query, map[string]field.Field{"field": f1}, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)

			vals := make([]uint32, 0)
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}

func Test_execBoolMust(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []uint32
	}{
		{
			name:      "empty_query",
			query:     `[]`,
			erroneous: false,
			expected:  []uint32{},
		},
		{
			name: "ok_same_values",
			query: `
			[
				{
					"term": {
						"field": "1"
					}
				},
				{
					"term": {
						"field": "1"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name: "ok_intersection",
			query: `
			[
				{
					"terms": {
						"field": ["1", "2"]
					}
				},
				{
					"term": {
						"field": "1"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name: "ok_nothing_found",
			query: `
			[
				{
					"term": {
						"field": "3"
					}
				},
				{
					"term": {
						"field": "4"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1, err := field.NewKeyword(context.Background(), "")
			require.Nil(t, err)
			err = f1.AddValueSync(1, "1")
			require.Nil(t, err)
			err = f1.AddValueSync(2, "2")
			require.Nil(t, err)

			query, err := decodeQuerySlice(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := execBoolMust(query, map[string]field.Field{"field": f1}, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)

			vals := make([]uint32, 0)
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}

func Test_execBoolFilter(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []uint32
	}{
		{
			name:      "empty_query",
			query:     `[]`,
			erroneous: false,
			expected:  []uint32{},
		},
		{
			name: "ok_same_values",
			query: `
			[
				{
					"term": {
						"field": "1"
					}
				},
				{
					"term": {
						"field": "1"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name: "ok_intersection",
			query: `
			[
				{
					"terms": {
						"field": ["1", "2"]
					}
				},
				{
					"term": {
						"field": "1"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name: "ok_nothing_found",
			query: `
			[
				{
					"term": {
						"field": "3"
					}
				},
				{
					"term": {
						"field": "4"
					}
				}
			]
			`,
			erroneous: false,
			expected:  []uint32{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1, err := field.NewKeyword(context.Background(), "")
			require.Nil(t, err)
			err = f1.AddValueSync(1, "1")
			require.Nil(t, err)
			err = f1.AddValueSync(2, "2")
			require.Nil(t, err)

			query, err := decodeQuerySlice(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := execBoolFilter(query, map[string]field.Field{"field": f1}, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)

			vals := make([]uint32, 0)
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}
