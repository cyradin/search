package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_TermQuery_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request field is not defined", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"query": "query"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query is not defined", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query not a stringable value", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": []
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must not return error if request is valid", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": "query"
		}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})
}

func Test_TermQuery_Exec(t *testing.T) {
	f, err := field.New(schema.TypeKeyword)
	require.NoError(t, err)
	f.Add(1, "value")

	t.Run("must return empty result if field not found", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field1",
			"query": "value"
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return empty result if value not found", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": "value1"
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return non-empty result if value is found", func(t *testing.T) {
		query := new(TermQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": "value"
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.False(t, result.Docs().IsEmpty())
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
}

func Test_TermsQuery_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request field is not defined", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"query": ["query"]
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query is not defined", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field"
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query is empty", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": []
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must return error if request query items are not stringable values", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": [{}]
		}`, query)

		err := validation.Validate(query)
		require.Error(t, err)
	})
	t.Run("must not return error if request is valid", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": ["query"]
		}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})
}

func Test_TermsQuery_Exec(t *testing.T) {
	f, err := field.New(schema.TypeKeyword)
	require.NoError(t, err)
	f.Add(1, "value")

	t.Run("must return empty result if field not found", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field1",
			"query": ["value"]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return empty result if value not found", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": ["value1"]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.True(t, result.Docs().IsEmpty())
	})

	t.Run("must return non-empty result if value is found", func(t *testing.T) {
		query := new(TermsQuery)
		mustUnmarshal(t, `{
			"field": "field",
			"query": ["value"]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f})
		require.NoError(t, err)
		require.False(t, result.Docs().IsEmpty())
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
}
