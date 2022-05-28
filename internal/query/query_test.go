package query

import (
	"context"
	"strings"
	"testing"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func decodeQuery(query string) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	dec := jsoniter.NewDecoder(strings.NewReader(query))
	dec.UseNumber()
	err := dec.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func decodeQuerySlice(query string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	dec := jsoniter.NewDecoder(strings.NewReader(query))
	dec.UseNumber()
	err := dec.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func Test_Exec(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []entity.SearchHit
	}{
		{
			name:      "error",
			query:     `{"term":{}}`,
			erroneous: true,
		},
		{
			name:      "ok",
			query:     `{"term":{ "field": 1 }}`,
			erroneous: false,
			expected:  []entity.SearchHit{{ID: 1}},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := field.NewBool(context.Background(), "")
			err := f.AddValueSync(1, true)
			require.Nil(t, err)

			query, err := decodeQuery(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := Exec(query, map[string]field.Field{"field": f})
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)
			require.EqualValues(t, d.expected, result)
		})
	}
}

func Test_exec(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
		expected  []uint32
	}{
		{
			name:      "error_empty_query",
			query:     `{}`,
			erroneous: true,
		},
		{
			name:      "error_multi_query",
			query:     `{"term":{ "field": 1 }, "terms":{ "field": [1] }}`,
			erroneous: true,
		},
		{
			name:      "error_invalid_query",
			query:     `{"term":1 }`,
			erroneous: true,
		},
		{
			name:      "error_unknown_query",
			query:     `{"invalid": { "field": 1 }}`,
			erroneous: true,
		},
		{
			name:      "ok_term",
			query:     `{"term":{ "field": true }}`,
			erroneous: false,
			expected:  []uint32{1},
		},
		{
			name:      "ok_terms",
			query:     `{"terms":{ "field": [true,false] }}`,
			erroneous: false,
			expected:  []uint32{1, 2},
		},
		{
			name: "ok_bool",
			query: `
			{
				"bool": {
					"should": [
						{
							"terms": {
								"field": [
									true,
									false
								]
							}
						}
					]
				}
			}
			`,
			erroneous: false,
			expected:  []uint32{1, 2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1 := field.NewBool(context.Background(), "")
			err := f1.AddValueSync(1, true)
			require.Nil(t, err)
			err = f1.AddValueSync(2, false)
			require.Nil(t, err)

			query, err := decodeQuery(d.query)
			require.Nil(t, err)
			require.Nil(t, err)

			result, err := exec(query, map[string]field.Field{"field": f1}, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, result)
				return
			}

			require.Nil(t, err)

			var vals []uint32
			result.Iterate(func(x uint32) bool {
				vals = append(vals, x)
				return true
			})
			require.EqualValues(t, d.expected, vals)
		})
	}
}
