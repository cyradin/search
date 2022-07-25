package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Bool(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			field := NewBool()
			field.Add(1, true)
			field.Add(1, false)
			field.Add(2, true)

			require.EqualValues(t, 2, field.dataTrue.GetCardinality())
			require.EqualValues(t, 1, field.dataFalse.GetCardinality())
			require.True(t, field.dataTrue.Contains(1))
			require.True(t, field.dataTrue.Contains(2))
			require.True(t, field.dataFalse.Contains(1))
			require.False(t, field.dataFalse.Contains(2))
		})
		t.Run("string", func(t *testing.T) {
			field := NewBool()
			field.Add(1, "qwe")

			require.EqualValues(t, 0, field.dataTrue.GetCardinality())
			require.EqualValues(t, 0, field.dataFalse.GetCardinality())
		})
	})

	t.Run("Term", func(t *testing.T) {
		field := NewBool()
		field.Add(1, true)

		result := field.Term(context.Background(), true)
		require.True(t, result.Docs().Contains(1))
		require.EqualValues(t, 1, result.Docs().GetCardinality())

		result = field.Term(context.Background(), false)
		require.False(t, result.Docs().Contains(1))
		require.EqualValues(t, 0, result.Docs().GetCardinality())
	})

	t.Run("Delete", func(t *testing.T) {
		field := NewBool()
		field.Add(1, true)
		field.Add(1, false)
		field.Add(2, false)

		field.Delete(2)
		require.EqualValues(t, 1, field.dataTrue.GetCardinality())
		require.EqualValues(t, 1, field.dataFalse.GetCardinality())

		field.Delete(1)
		require.EqualValues(t, 0, field.dataTrue.GetCardinality())
		require.EqualValues(t, 0, field.dataFalse.GetCardinality())
	})

	t.Run("Data", func(t *testing.T) {
		field := NewBool()
		field.Add(1, true)
		field.Add(1, false)
		field.Add(2, false)

		result := field.Data(1)
		require.EqualValues(t, []interface{}{true, false}, result)

		result = field.Data(2)
		require.EqualValues(t, []interface{}{false}, result)
	})

	t.Run("MarshalBinary-UnmarshalBinary", func(t *testing.T) {
		field := NewBool()
		field.Add(1, true)
		field.Add(1, false)
		field.Add(2, true)

		data, err := field.MarshalBinary()
		require.NoError(t, err)

		field2 := NewBool()
		err = field2.UnmarshalBinary(data)
		require.NoError(t, err)
		require.True(t, field2.dataTrue.Contains(1))
		require.True(t, field2.dataFalse.Contains(1))
		require.True(t, field2.dataTrue.Contains(2))
	})
}
