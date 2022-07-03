package field

import (
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
		s := schema.New(map[string]schema.Field{
			"f1": {Type: schema.TypeBool},
			"f2": {Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		index.Add(1, map[string]interface{}{"f1": true})
		index.Add(2, map[string]interface{}{"f2": true})

		f1 := index.fields["f1"].GetValue(true)
		require.True(t, f1.Contains(1))
		require.False(t, f1.Contains(2))

		f2 := index.fields["f2"].GetValue(true)
		require.False(t, f2.Contains(1))
		require.True(t, f2.Contains(2))

		all := index.fields[AllField].GetValue(true)
		require.True(t, all.Contains(1))
		require.True(t, all.Contains(2))
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
