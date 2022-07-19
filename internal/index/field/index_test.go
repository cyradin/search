package field

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_index(t *testing.T) {
	t.Run("can create new index", func(t *testing.T) {

		s := schema.New(map[string]schema.Field{
			"bool": {Type: schema.TypeBool},
			"text": {Type: schema.TypeText, Analyzer: "analyzer"},
		}, map[string]schema.FieldAnalyzer{
			"analyzer": {Analyzers: []schema.Analyzer{{Type: schema.TokenizerWhitespace}}},
		})
		index, err := NewIndex("name", s)
		require.NoError(t, err)
		require.NotEqual(t, s.Fields, index.fields)
		require.Contains(t, index.fields, "bool")
		require.Contains(t, index.fields, "text")
		require.Contains(t, index.fields, AllField)
	})

	t.Run("can add document", func(t *testing.T) {
		ctx := context.Background()

		s := schema.New(map[string]schema.Field{
			"f1": {Type: schema.TypeBool},
			"f2": {Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		index.Add(1, map[string]interface{}{"f1": true})
		index.Add(2, map[string]interface{}{"f2": true})

		result1 := index.fields["f1"].Get(ctx, true)
		require.True(t, result1.Docs().Contains(1))
		require.False(t, result1.Docs().Contains(2))

		result2 := index.fields["f2"].Get(ctx, true)
		require.False(t, result2.Docs().Contains(1))
		require.True(t, result2.Docs().Contains(2))

		result3 := index.fields[AllField].Get(ctx, true)
		require.True(t, result3.Docs().Contains(1))
		require.True(t, result3.Docs().Contains(2))
	})

	t.Run("can get all fields", func(t *testing.T) {
		s := schema.New(map[string]schema.Field{
			"f1": {Type: schema.TypeBool},
			"f2": {Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		fields := index.Fields()
		require.EqualValues(t, index.fields, fields)
	})
}
