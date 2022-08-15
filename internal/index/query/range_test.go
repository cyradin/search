package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_RangeQuery_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := new(RangeQuery)
		mustUnmarshal(t, `{}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request field is not defined", func(t *testing.T) {
		query := new(RangeQuery)
		mustUnmarshal(t, `{
			"from": "10"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request 'from' and 'to' are empty", func(t *testing.T) {
		query := new(RangeQuery)
		mustUnmarshal(t, `{
			"field": "field"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must not return error if request is a valid query", func(t *testing.T) {
		t.Run("only from", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"from": 10
			}`, query)

			err := validation.Validate(query)
			require.NoError(t, err)
		})
		t.Run("only to", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"to": 50
			}`, query)

			err := validation.Validate(query)
			require.NoError(t, err)
		})
		t.Run("both from and to to", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"to": 10,
				"to": 50
			}`, query)

			err := validation.Validate(query)
			require.NoError(t, err)
		})
		t.Run("both from and to including range limits", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"to": 10,
				"to": 50,
				"includeFrom": true,
				"includeTo": true
			}`, query)

			err := validation.Validate(query)
			require.NoError(t, err)
		})
	})
}

func Test_RangeQuery_Exec(t *testing.T) {
	f, err := field.New(schema.TypeInteger)
	require.NoError(t, err)
	f.Add(1, 1)
	f.Add(2, 2)
	f.Add(3, 3)
	f.Add(4, 4)
	f.Add(5, 5)
	ctx := withFields(context.Background(),
		map[string]field.Field{
			"field": f,
		},
	)

	t.Run("must return empty result if field not found", func(t *testing.T) {
		query := new(RangeQuery)
		mustUnmarshal(t, `{
				"field": "field1",
				"from": 1,
				"to": 3
			}`, query)

		result, err := query.Exec(ctx)
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return valid result", func(t *testing.T) {
		t.Run("[1, 3]", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"from": 1,
				"to": 3,
				"includeFrom": true,
				"includeTo": true
			}`, query)

			result, err := query.Exec(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
		})

		t.Run("(1, 4)", func(t *testing.T) {
			query := new(RangeQuery)
			mustUnmarshal(t, `{
				"field": "field",
				"from": 1,
				"to": 4
			}`, query)

			result, err := query.Exec(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []uint32{2, 3}, result.Docs().ToArray())
		})
	})
}
