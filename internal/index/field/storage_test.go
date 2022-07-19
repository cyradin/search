package field

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_Storage(t *testing.T) {
	schemaBoolField := schema.New(map[string]schema.Field{
		"bool": schema.NewField(schema.TypeBool, false, ""),
	}, nil)
	schemaTextField := schema.New(map[string]schema.Field{
		"text": schema.NewField(schema.TypeText, false, "analyzer"),
	}, map[string]schema.FieldAnalyzer{
		"analyzer": {Analyzers: []schema.Analyzer{{Type: schema.TokenizerWhitespace}}},
	})

	t.Run("can add new index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schemaBoolField)
		require.NoError(t, err)
		require.NotNil(t, index)
		require.Equal(t, s.indexes["name"], index)
	})

	t.Run("cannot add an existing index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schemaBoolField)
		require.NoError(t, err)
		require.NotNil(t, index)

		index, err = s.AddIndex("name", schemaBoolField)
		require.Error(t, err)
		require.Nil(t, index)
	})

	t.Run("can delete index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schemaBoolField)
		require.NoError(t, err)
		require.NotNil(t, index)

		s.DeleteIndex("name")
		require.Nil(t, s.indexes["name"])
	})

	t.Run("no error when deleting non-existent index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		s.DeleteIndex("name")
		require.Nil(t, s.indexes["name"])
	})

	t.Run("can get existing index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schemaBoolField)
		require.NoError(t, err)
		require.NotNil(t, index)

		index, err = s.GetIndex("name")
		require.NoError(t, err)
		require.Equal(t, s.indexes["name"], index)
	})

	t.Run("error getting non-existent index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.GetIndex("name")
		require.Error(t, err)
		require.Nil(t, index)
	})

	t.Run("can load index from file", func(t *testing.T) {
		t.Run("bool field", func(t *testing.T) {
			ctx := context.Background()

			dir := t.TempDir()
			s := NewStorage(dir)

			fieldsDir := s.indexFieldsDir("name")

			err := os.MkdirAll(fieldsDir, dirPermissions)
			require.NoError(t, err)

			field := NewBool()
			field.Add(1, true)
			data, err := field.MarshalBinary()
			require.NoError(t, err)
			err = os.WriteFile(path.Join(fieldsDir, "bool"+fieldFileExt), data, filePermissions)
			require.NoError(t, err)

			index, err := NewIndex("name", schemaBoolField)
			require.NoError(t, err)
			err = s.loadIndex(index)
			require.NoError(t, err)

			result := index.fields["bool"].Get(ctx, true)
			require.True(t, result.Docs().Contains(1))
		})
		t.Run("text field", func(t *testing.T) {
			ctx := context.Background()

			dir := t.TempDir()
			s := NewStorage(dir)

			fieldsDir := s.indexFieldsDir("name")

			err := os.MkdirAll(fieldsDir, dirPermissions)
			require.NoError(t, err)

			scoring := NewScoring()
			scoring.data.AvgDocLen = 5

			field := NewText(func(s []string) []string { return s }, scoring)
			field.Add(1, "word")
			data, err := field.MarshalBinary()
			require.NoError(t, err)
			err = os.WriteFile(path.Join(fieldsDir, "text"+fieldFileExt), data, filePermissions)
			require.NoError(t, err)

			index, err := NewIndex("name", schemaTextField)
			require.NoError(t, err)
			err = s.loadIndex(index)
			require.NoError(t, err)

			result := index.fields["text"].Get(ctx, "word")
			require.True(t, result.Docs().Contains(1))
		})
	})

	t.Run("can dump index to file", func(t *testing.T) {
		t.Run("bool field", func(t *testing.T) {
			dir := t.TempDir()
			s := NewStorage(dir)

			index, err := s.AddIndex("name1", schemaBoolField)
			require.NoError(t, err)
			require.NotNil(t, index)

			err = s.dumpIndex(index)
			require.NoError(t, err)

			_, err = os.Stat(path.Join(s.indexFieldsDir(index.name), AllField+fieldFileExt))
			require.NoError(t, err)
			_, err = os.Stat(path.Join(s.indexFieldsDir(index.name), "bool"+fieldFileExt))
			require.NoError(t, err)
		})
		t.Run("text field", func(t *testing.T) {
			dir := t.TempDir()
			s := NewStorage(dir)

			index, err := s.AddIndex("name", schemaTextField)
			require.NoError(t, err)
			require.NotNil(t, index)

			err = s.dumpIndex(index)
			require.NoError(t, err)

			_, err = os.Stat(path.Join(s.indexFieldsDir(index.name), AllField+fieldFileExt))
			require.NoError(t, err)
			_, err = os.Stat(path.Join(s.indexFieldsDir(index.name), "text"+fieldFileExt))
			require.NoError(t, err)
		})
	})

	t.Run("can dump all indexes on app stop", func(t *testing.T) {
		t.Run("bool field", func(t *testing.T) {
			dir := t.TempDir()
			s := NewStorage(dir)
			index1, err := s.AddIndex("name1", schemaBoolField)
			require.NoError(t, err)
			require.NotNil(t, index1)
			index2, err := s.AddIndex("name2", schemaBoolField)
			require.NoError(t, err)
			require.NotNil(t, index2)

			events.Dispatch(context.Background(), events.NewAppStop())

			_, err = os.Stat(path.Join(dir, index1.name, fieldsDir, "bool"+fieldFileExt))
			require.NoError(t, err)

			_, err = os.Stat(path.Join(dir, index2.name, fieldsDir, "bool"+fieldFileExt))
			require.NoError(t, err)
		})
	})
}
