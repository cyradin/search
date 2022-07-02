package field

import (
	"os"
	"path"
	"testing"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_index(t *testing.T) {
	t.Run("can create new index", func(t *testing.T) {

		s := schema.New(map[string]schema.Field{
			"bool": {Name: "bool", Type: schema.TypeBool},
			"text": {Name: "text", Type: schema.TypeText, Analyzer: "analyzer"},
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

	t.Run("can load data from file", func(t *testing.T) {
		dir := t.TempDir()

		field := NewBool()
		field.AddValue(1, true)
		data, err := field.MarshalBinary()
		require.NoError(t, err)
		err = os.WriteFile(path.Join(dir, "bool"+fieldFileExt), data, filePermissions)
		require.NoError(t, err)

		s := schema.New(map[string]schema.Field{
			"bool": {Name: "bool", Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)
		err = index.load(dir)
		require.NoError(t, err)

		val, ok := index.fields["bool"].GetValue(true)
		require.True(t, ok)
		require.True(t, val.Contains(1))
	})

	t.Run("can dump data to file", func(t *testing.T) {
		dir := t.TempDir()
		s := schema.New(map[string]schema.Field{
			"bool": {Name: "bool", Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		index.fields["bool"].AddValue(1, true)

		err = index.dump(dir)
		require.NoError(t, err)

		_, err = os.Stat(path.Join(dir, AllField+fieldFileExt))
		require.NoError(t, err)
		_, err = os.Stat(path.Join(dir, "bool"+fieldFileExt))
		require.NoError(t, err)

		index2, err := NewIndex("name", s)
		require.NoError(t, err)
		err = index2.load(dir)
		require.NoError(t, err)

		require.Contains(t, index2.fields, "bool")
		val, ok := index2.fields["bool"].GetValue(true)
		require.True(t, ok)
		require.True(t, val.Contains(1))
	})

	t.Run("can add document", func(t *testing.T) {
		s := schema.New(map[string]schema.Field{
			"f1": {Name: "f1", Type: schema.TypeBool},
			"f2": {Name: "f2", Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		index.Add(1, map[string]interface{}{"f1": true})
		index.Add(2, map[string]interface{}{"f2": true})

		f1, ok := index.fields["f1"].GetValue(true)
		require.True(t, ok)
		require.True(t, f1.Contains(1))
		require.False(t, f1.Contains(2))

		f2, ok := index.fields["f2"].GetValue(true)
		require.True(t, ok)
		require.False(t, f2.Contains(1))
		require.True(t, f2.Contains(2))

		all, ok := index.fields[AllField].GetValue(true)
		require.True(t, ok)
		require.True(t, all.Contains(1))
		require.True(t, all.Contains(2))
	})

	t.Run("can get all fields", func(t *testing.T) {
		s := schema.New(map[string]schema.Field{
			"f1": {Name: "f1", Type: schema.TypeBool},
			"f2": {Name: "f2", Type: schema.TypeBool},
		}, nil)
		index, err := NewIndex("name", s)
		require.NoError(t, err)

		fields := index.Fields()
		require.EqualValues(t, index.fields, fields)
	})
}
