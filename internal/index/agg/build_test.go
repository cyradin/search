package agg

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func mustUnmarshal(t *testing.T, src string, dst interface{}) {
	err := jsoniter.Unmarshal([]byte(src), dst)
	require.NoError(t, err)
}

func Test_build(t *testing.T) {
	t.Run("must return empty result if request is nil", func(t *testing.T) {
		result, err := build(nil)
		require.NoError(t, err)
		require.Equal(t, Aggs{}, result)
	})

	t.Run("must return empty result if request is an empty map", func(t *testing.T) {
		result, err := build(make(AggsRequest))
		require.NoError(t, err)
		require.Equal(t, Aggs{}, result)
	})

	t.Run("must return validation error if request agg is not an object", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
				"aggname": 1
			}`, &req)

		result, err := build(req)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request agg type is empty", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {}
		}`, &req)

		result, err := build(req)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request agg type not a string", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {
				"type": {}
			}
		}`, &req)

		result, err := build(req)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return validation error if request agg type is unknown", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {
				"type": "invalid"
			}
		}`, &req)

		result, err := build(req)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must not return validation error if request is a valid terms aggregation", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {
				"type": "terms",
				"size": 10,
				"field": "value"
			}
		}`, &req)

		result, err := build(req)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return validation error if request contains valid subaggregations", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {
				"type": "terms",
				"size": 10,
				"field": "value",
				"aggs": {
					"subaggname": {
						"type": "terms",
						"size": 10,
						"field": "value"
					}
				}
			}
		}`, &req)

		result, err := build(req)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return validation error if request is a valid range aggregation", func(t *testing.T) {
		req := make(AggsRequest)
		mustUnmarshal(t, `{
			"aggname": {
				"type": "range",
				"ranges": [
					{
						"key": "key",
						"from": 10,
						"to": 50
					}
				],
				"field": "value"
			}
		}`, &req)

		result, err := build(req)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}
