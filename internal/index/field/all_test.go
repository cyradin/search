package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type testFieldValue struct {
	id    uint32
	value interface{}
}

func Test_All(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		field := NewAll()
		field.Add(1, true)

		require.True(t, field.data.Contains(1))
		require.False(t, field.data.Contains(2))
	})

	t.Run("Term", func(t *testing.T) {
		field := NewAll()
		field.Add(1, true)

		result := field.Term(context.Background(), true)
		require.True(t, result.Docs().Contains(1))
	})

	t.Run("GetAnd", func(t *testing.T) {
		field := NewAll()
		field.Add(1, true)

		result := field.GetAnd(context.Background(), nil)
		require.True(t, result.Docs().Contains(1))
	})

	t.Run("GetOr", func(t *testing.T) {
		field := NewAll()
		field.Add(1, true)

		result := field.GetOr(context.Background(), nil)
		require.True(t, result.Docs().Contains(1))
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("can delete value", func(t *testing.T) {
			field := NewAll()
			field.Add(1, true)

			field.Delete(1)
			require.EqualValues(t, 0, field.data.GetCardinality())
		})

		t.Run("cannot delete other values", func(t *testing.T) {
			field := NewAll()
			field.Add(1, true)
			field.Add(2, true)

			field.Delete(1)
			require.EqualValues(t, 1, field.data.GetCardinality())
			require.True(t, field.data.Contains(2))
		})
	})

	t.Run("MarshalBinary-UnmarshalBinary", func(t *testing.T) {
		field := NewAll()
		field.Add(1, true)

		data, err := field.MarshalBinary()
		require.NoError(t, err)

		field2 := NewAll()
		err = field2.UnmarshalBinary(data)
		require.NoError(t, err)
		require.True(t, field2.data.Contains(1))
	})
}
