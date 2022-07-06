package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_matchQuery(t *testing.T) {
	t.Run("newMatchQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
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

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field contains extra keys", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello",
					"qwerty": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request is a valid query", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newMatchQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})
}
