package agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_newTermAgg(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := `{}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request does not contain 'size' key", func(t *testing.T) {
		query := `{
			"field": "field"
		}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request size key is < 0", func(t *testing.T) {
		query := `{
				"field": "field",
				"size": -1
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request size key == 0", func(t *testing.T) {
		query := `{
				"field": "field",
				"size": 0
			}`
		req := mustDecodeRequest(t, query)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request does not contain 'field' key", func(t *testing.T) {
		query := `{
				"size": 1
			}`
		req, err := decodeRequest(query)
		require.NoError(t, err)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request 'field' value is not a string", func(t *testing.T) {
		query := `{
				"field": true,
				"size": 1
			}`
		req, err := decodeRequest(query)
		require.NoError(t, err)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must return error if request 'field' value is empty", func(t *testing.T) {
		query := `{
				"field": "",
				"size": 1
			}`
		req, err := decodeRequest(query)
		require.NoError(t, err)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.Error(t, err)
		require.Nil(t, q)
	})

	t.Run("must not return error if request is a valid term aggregation", func(t *testing.T) {
		query := `{
				"field": "field",
				"size": 1
			}`
		req, err := decodeRequest(query)
		require.NoError(t, err)

		q, err := newTermsAgg(context.Background(), req, nil)
		require.NoError(t, err)
		require.NotNil(t, q)
	})
}
