package agg

import (
	"context"
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func decodeRequest(query string) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	dec := jsoniter.NewDecoder(strings.NewReader(query))
	dec.UseNumber()
	err := dec.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func Test_build(t *testing.T) {
	t.Run("must return empty result if request is nil", func(t *testing.T) {
		result, err := build(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, map[string]internalAgg{}, result)
	})

	t.Run("must return empty result if request is an empty map", func(t *testing.T) {
		result, err := build(context.Background(), make(Aggs))
		require.NoError(t, err)
		require.Equal(t, map[string]internalAgg{}, result)
	})

	t.Run("must return validation error if request agg is not an object", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": 1
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request agg definition is not an object", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": 1
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request agg contains multiple definitions", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				},
				"range": {
					"field": "value",
					"ranges": [
						{
							"from": 50,
							"to": 100
						}
					]
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request contains invalid agg type", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"invalid": {
					"size": 10,
					"field": "value"
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must not return validation error if request is a valid aggregation", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return validation error if request contains subaggregations", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				},
				"aggs": {
					"aggname": {
						"term": {
							"size": 10,
							"field": "value"
						}
					}
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must return validation error if request contains invalid agg in subggregations", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				},
				"aggs": 1
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request contains invalid agg type in subggregations", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				},
				"aggs": {
					"aggname": 1
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request contains invalid agg definition in subggregations", func(t *testing.T) {
		aggs, err := decodeRequest(`{
			"aggname": {
				"terms": {
					"size": 10,
					"field": "value"
				},
				"aggs": {
					"aggname": {
						"invalid": {
							"size": 10,
							"field": "value"
						}
					}
				}
			}
		}`)

		result, err := build(context.Background(), aggs)
		require.Error(t, err)
		require.Nil(t, result)
	})
}
