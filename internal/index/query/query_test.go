package query

import (
	"strings"
	"testing"

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
		expected  []SearchHit
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
			expected:  []SearchHit{{ID: 1}},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := field.NewBool()
			f.Add(1, true)

			req, err := decodeQuery(d.query)
			require.NoError(t, err)
			require.NoError(t, err)

			result, err := Exec(req, map[string]field.Field{"field": f})
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.EqualValues(t, d.expected, result)
		})
	}
}

func Test_build(t *testing.T) {
	data := []struct {
		name      string
		query     string
		erroneous bool
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
		},
		{
			name:      "ok_terms",
			query:     `{"terms":{ "field": [true,false] }}`,
			erroneous: false,
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
		},
		{
			name: "err_bool_subquery",
			query: `
					{
						"bool": {
							"should": [
								{
									"invalid": {
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
			erroneous: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f1 := field.NewBool()

			query, err := decodeQuery(d.query)
			require.NoError(t, err)

			result, err := build(query, map[string]field.Field{"field": f1}, "query")
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}
