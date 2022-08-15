package query

import (
	"bytes"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func mustUnmarshal(t *testing.T, src string, dst interface{}) {
	dec := jsoniter.NewDecoder(bytes.NewBuffer([]byte(src)))
	dec.UseNumber()
	err := dec.Decode(dst)
	require.NoError(t, err)
}

func Test_build(t *testing.T) {
	t.Run("must return error if query is empty", func(t *testing.T) {
		query := []byte(`{}`)

		result, err := build(query)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return error if query type is unknown", func(t *testing.T) {
		query := []byte(`{
			"type": "invalid"
		}`)

		result, err := build(query)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must return error if query is invalid", func(t *testing.T) {
		query := []byte(`{
			"type": "term",
			"field": "field"
		}`)

		result, err := build(query)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("must not return error if query is a valid term query", func(t *testing.T) {
		query := []byte(`{
			"type": "term",
			"field": "field",
			"query": "query"
		}`)

		result, err := build(query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid terms query", func(t *testing.T) {
		query := []byte(`{
			"type": "terms",
			"field": "field",
			"query": ["query"]
		}`)

		result, err := build(query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid bool query", func(t *testing.T) {
		query := []byte(`{
			"type": "bool"
		}`)

		result, err := build(query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid match query", func(t *testing.T) {
		query := []byte(`{
			"type": "match",
			"field": "field",
			"query": "query"
		}`)

		result, err := build(query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("must not return error if query is a valid range query", func(t *testing.T) {
		query := []byte(`{
			"type": "range",
			"field": "field",
			"from": 10
		}`)

		result, err := build(query)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}
