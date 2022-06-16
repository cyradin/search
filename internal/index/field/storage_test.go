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
	t.Run("can create new storage", func(t *testing.T) {
		dir := t.TempDir()

		s := schema.New([]schema.Field{
			{Name: "bool", Type: schema.TypeBool},
		})
		storage, err := NewStorage(dir, s)
		require.Nil(t, err)
		require.NotEqual(t, s.Fields, storage.fields)
		require.Contains(t, storage.fields, "bool")
		require.Contains(t, storage.fields, AllField)
	})

	t.Run("can load data from file", func(t *testing.T) {
		dir := t.TempDir()

		field := NewBool()
		field.AddValue(1, true)
		data, err := field.MarshalBinary()
		require.NoError(t, err)
		err = os.WriteFile(path.Join(dir, "bool"+fileExt), data, filePermissions)
		require.NoError(t, err)

		s := schema.New([]schema.Field{
			{Name: "bool", Type: schema.TypeBool},
		})
		storage, err := NewStorage(dir, s)
		require.Nil(t, err)
		require.NotEqual(t, s.Fields, storage.fields)
		require.Contains(t, storage.fields, "bool")
		require.Contains(t, storage.fields, AllField)

		val, ok := storage.fields["bool"].GetValue(true)
		require.True(t, ok)
		require.True(t, val.Contains(1))
	})

	t.Run("can dump data to file on app stop", func(t *testing.T) {
		dir := t.TempDir()
		s := schema.New([]schema.Field{
			{Name: "bool", Type: schema.TypeBool},
		})
		storage, err := NewStorage(dir, s)
		require.Nil(t, err)
		require.NotEqual(t, s.Fields, storage.fields)
		require.Contains(t, storage.fields, "bool")
		require.Contains(t, storage.fields, AllField)

		storage.fields["bool"].AddValue(1, true)

		events.Dispatch(context.Background(), events.NewAppStop())

		_, err = os.Stat(path.Join(dir, AllField+fileExt))
		require.NoError(t, err)
		_, err = os.Stat(path.Join(dir, "bool"+fileExt))
		require.NoError(t, err)

		storage2, err := NewStorage(dir, s)
		require.Nil(t, err)
		require.Contains(t, storage2.fields, "bool")
		val, ok := storage2.fields["bool"].GetValue(true)
		require.True(t, ok)
		require.True(t, val.Contains(1))
	})
}
