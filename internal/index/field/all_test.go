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

func Test_All_Add(t *testing.T) {
	field := NewAll()
	field.Add(1, true)

	require.True(t, field.data.Contains(1))
	require.False(t, field.data.Contains(2))
}

func Test_All_Term(t *testing.T) {
	field := NewAll()
	field.Add(1, true)

	result := field.Term(context.Background(), true)
	require.True(t, result.Docs().Contains(1))
}

func Test_All_Match(t *testing.T) {
	field := NewAll()
	field.Add(1, true)

	result := field.Match(context.Background(), true)
	require.True(t, result.Docs().Contains(1))
}

func Test_All_Delete(t *testing.T) {
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
}

func Test_All_Marshal(t *testing.T) {
	field := NewAll()
	field.Add(1, true)

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := NewAll()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.data.Contains(1))
}
