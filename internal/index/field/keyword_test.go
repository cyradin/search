package field

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Keyword_Add(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			field := NewKeyword()
			field.Add(1, true)

			require.EqualValues(t, 1, field.data["true"].GetCardinality())
			require.True(t, field.data["true"].Contains(1))
		})
		t.Run("string", func(t *testing.T) {
			field := NewKeyword()
			field.Add(1, "foo")
			field.Add(1, "bar")
			field.Add(2, "foo")

			require.EqualValues(t, 2, field.data["foo"].GetCardinality())
			require.EqualValues(t, 1, field.data["bar"].GetCardinality())
			require.True(t, field.data["foo"].Contains(1))
			require.True(t, field.data["foo"].Contains(2))
			require.True(t, field.data["bar"].Contains(1))
			require.False(t, field.data["bar"].Contains(2))
		})
	})

	t.Run("Get", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")

		result := field.Get(context.Background(), "foo")
		require.True(t, result.Docs().Contains(1))
		require.EqualValues(t, 1, result.Docs().GetCardinality())

		result = field.Get(context.Background(), "bar")
		require.False(t, result.Docs().Contains(1))
		require.EqualValues(t, 0, result.Docs().GetCardinality())
	})

	t.Run("GetOr", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")
		field.Add(2, "bar")
		field.Add(3, "baz")

		result := field.GetOr(context.Background(), []interface{}{"foo", "bar"})
		require.True(t, result.Docs().Contains(1))
		require.True(t, result.Docs().Contains(2))
		require.EqualValues(t, 2, result.Docs().GetCardinality())
	})

	t.Run("GetAnd", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")
		field.Add(3, "baz")

		result := field.GetAnd(context.Background(), []interface{}{"foo", "bar"})
		require.True(t, result.Docs().Contains(1))
		require.EqualValues(t, 1, result.Docs().GetCardinality())
	})

	t.Run("Delete", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		field.Delete(2)
		require.EqualValues(t, 1, field.data["foo"].GetCardinality())
		require.EqualValues(t, 1, field.data["bar"].GetCardinality())
		require.ElementsMatch(t, []string{"foo", "bar"}, field.values[1])
		require.Nil(t, field.values[2])

		field.Delete(1)
		require.Nil(t, field.data["foo"])
		require.Nil(t, field.data["bar"])
		require.Nil(t, field.values[1])
	})

	t.Run("Data", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		result := field.Data(1)
		require.EqualValues(t, []interface{}{"foo", "bar"}, result)

		result = field.Data(2)
		require.EqualValues(t, []interface{}{"foo"}, result)
	})

	t.Run("MarshalBinary-UnmarshalBinary", func(t *testing.T) {
		field := NewKeyword()
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		data, err := field.MarshalBinary()
		require.NoError(t, err)

		field2 := NewKeyword()
		err = field2.UnmarshalBinary(data)
		require.NoError(t, err)
		require.True(t, field2.data["foo"].Contains(1))
		require.True(t, field2.data["bar"].Contains(1))
		require.ElementsMatch(t, []string{"foo", "bar"}, field.values[1])
		require.True(t, field2.data["foo"].Contains(2))
		require.ElementsMatch(t, []string{"foo"}, field.values[2])
	})
}
