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
	t.Run("can add new index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schema.New([]schema.Field{
			schema.NewField("bool", schema.TypeBool, false),
		}))
		require.NoError(t, err)
		require.NotNil(t, index)
		require.Equal(t, s.fields["name"], index)
	})

	t.Run("cannot add an existing index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schema.New([]schema.Field{}))
		require.NoError(t, err)
		require.NotNil(t, index)

		index, err = s.AddIndex("name", schema.New([]schema.Field{}))
		require.Error(t, err)
		require.Nil(t, index)
	})

	t.Run("can delete index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schema.New([]schema.Field{
			schema.NewField("bool", schema.TypeBool, false),
		}))
		require.NoError(t, err)
		require.NotNil(t, index)

		err = s.DeleteIndex("name")
		require.NoError(t, err)
		require.Nil(t, s.fields["name"])
	})

	t.Run("no error when deleting non-existent index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		err := s.DeleteIndex("name")
		require.NoError(t, err)
		require.Nil(t, s.fields["name"])
	})

	t.Run("can get existing index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.AddIndex("name", schema.New([]schema.Field{
			schema.NewField("bool", schema.TypeBool, false),
		}))
		require.NoError(t, err)
		require.NotNil(t, index)

		index, err = s.GetIndex("name")
		require.NoError(t, err)
		require.Equal(t, s.fields["name"], index)
	})

	t.Run("error getting non-existent index", func(t *testing.T) {
		s := NewStorage(t.TempDir())
		index, err := s.GetIndex("name")
		require.Error(t, err)
		require.Nil(t, index)
	})

	t.Run("can dump all indexes on app stop", func(t *testing.T) {
		dir := t.TempDir()
		s := NewStorage(dir)
		index1, err := s.AddIndex("name1", schema.New([]schema.Field{
			schema.NewField("bool", schema.TypeBool, false),
		}))
		require.NoError(t, err)
		require.NotNil(t, index1)
		index2, err := s.AddIndex("name2", schema.New([]schema.Field{
			schema.NewField("bool", schema.TypeBool, false),
		}))
		require.NoError(t, err)
		require.NotNil(t, index2)

		events.Dispatch(context.Background(), events.NewAppStop())

		_, err = os.Stat(path.Join(index1.src, "bool.gob"))
		require.NoError(t, err)

		_, err = os.Stat(path.Join(index2.src, "bool.gob"))
		require.NoError(t, err)
	})
}
