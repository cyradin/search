package query

import (
	"context"
	"strings"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
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

func Test_build(t *testing.T) {
	t.Run("must return error if query is empty", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.Error(t, err)
		require.Nil(t, result)
	})
	t.Run("must return error if query contains multiple root fields", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"term": {
				"field": 1
			},
			"terms": {
				"field": [1]
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.Error(t, err)
		require.Nil(t, result)
	})
	t.Run("must return error if query is invalid", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"term": {
				"field": 1
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.Error(t, err)
		require.Nil(t, result)
	})
	t.Run("must return error if query type is unknown", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"invalid": {
				"field": 1
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.Error(t, err)
		require.Nil(t, result)
	})
	t.Run("must not return error if query is a valid term query", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"term": {
				"field": {
					"query": "value"
				}
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
	t.Run("must not return error if query is a valid terms query", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"terms": {
				"field": {
					"query": ["value"]
				}
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
	t.Run("must not return error if query is a valid bool query", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"bool": {
				"should": [
					{
						"terms": {
							"field": {
								"query": ["value"]
							}
						}
					}
				]
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid match query", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"match": {
				"field": {
					"query": "hello"
				}
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid range query", func(t *testing.T) {
		f1, err := field.New(schema.TypeBool)
		require.NoError(t, err)

		query, err := decodeQuery(`{
			"range": {
				"field": {
					"from": 1
				}
			}
		}`)
		require.NoError(t, err)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1})
		result, err := build(ctx, query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}
