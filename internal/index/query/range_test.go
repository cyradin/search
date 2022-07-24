package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_rangeQuery(t *testing.T) {
	t.Run("newRangeQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newRangeQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request contains multiple keys", func(t *testing.T) {
			query := `{
				"field1": {},
				"field2": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newRangeQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newRangeQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field contains extra keys", func(t *testing.T) {
			query := `{
				"field1": {
					"from": 1,
					"qwerty": 1
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newRangeQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request is a valid query", func(t *testing.T) {
			query := `{
				"field1": {
					"from": 1,
					"includeLower": true,
					"to": 100,
					"includeUpper": true
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newRangeQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})
}
