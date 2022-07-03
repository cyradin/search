package field

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

func Test_genericField(t *testing.T) {
	t.Run("addValue", func(t *testing.T) {
		t.Run("can add one value", func(t *testing.T) {
			field := newField[bool](cast.ToBoolE)
			field.AddValue(1, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.Equal(t, uint64(1), bm.GetCardinality())
		})
		t.Run("can add two two different ids with different values", func(t *testing.T) {
			field := newField[bool](cast.ToBoolE)
			field.AddValue(1, true)
			field.AddValue(2, false)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.Equal(t, uint64(1), bm.GetCardinality())

			bm, ok = field.data[false]
			require.True(t, ok)
			require.True(t, bm.Contains(2))
			require.Equal(t, uint64(1), bm.GetCardinality())
		})
		t.Run("can add two two different ids with same value", func(t *testing.T) {
			field := newField[bool](cast.ToBoolE)
			field.AddValue(1, true)
			field.AddValue(2, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.True(t, bm.Contains(2))
			require.Equal(t, uint64(2), bm.GetCardinality())
		})
		t.Run("can overwrite value", func(t *testing.T) {
			field := newField[bool](cast.ToBoolE)
			field.AddValue(1, true)
			field.AddValue(1, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.Equal(t, uint64(1), bm.GetCardinality())
		})
	})

	t.Run("getValue", func(t *testing.T) {
		t.Run("can return value if found", func(t *testing.T) {
			f := newField[bool](cast.ToBoolE)
			f.data = map[bool]*roaring.Bitmap{
				true: roaring.New(),
			}
			f.data[true].Add(1)

			result := f.getValue(true)
			require.True(t, result.Contains(1))
			require.Equal(t, uint64(1), result.GetCardinality())
		})
		t.Run("can return empty bitmap if not found", func(t *testing.T) {
			f := newField[bool](cast.ToBoolE)
			f.data = map[bool]*roaring.Bitmap{
				true: roaring.New(),
			}
			f.data[true].Add(1)

			result := f.getValue(false)
			require.False(t, result.Contains(1))
			require.Equal(t, uint64(0), result.GetCardinality())
		})
		t.Run("can return empty bitmap if invalid value provided", func(t *testing.T) {
			f := newField[bool](cast.ToBoolE)
			f.data = map[bool]*roaring.Bitmap{
				true: roaring.New(),
			}
			f.data[true].Add(1)

			result := f.getValue("qwerty")
			require.False(t, result.Contains(1))
			require.Equal(t, uint64(0), result.GetCardinality())
		})
	})

	t.Run("getValuesOr", func(t *testing.T) {
		map1 := roaring.New()
		map1.Add(1)
		map1.Add(2)

		map2 := roaring.New()
		map2.Add(2)
		map2.Add(3)
		map2.Add(4)

		data := map[int]*roaring.Bitmap{
			0: map1,
			1: map2,
		}

		t.Run("can return union if both values found", func(t *testing.T) {
			f := newField[int](cast.ToIntE)
			f.data = data

			result := f.getValuesOr([]interface{}{0, 1})
			require.Equal(t, uint64(4), result.GetCardinality())
			require.True(t, result.Contains(1))
			require.True(t, result.Contains(2))
			require.True(t, result.Contains(3))
			require.True(t, result.Contains(4))
		})
		t.Run("can return non-empty result if at least one value found", func(t *testing.T) {
			f := newField[int](cast.ToIntE)
			f.data = data

			result := f.getValuesOr([]interface{}{0, "qwe"})
			require.Equal(t, uint64(2), result.GetCardinality())
			require.True(t, result.Contains(1))
			require.True(t, result.Contains(2))
		})
		t.Run("can return empty result if nothing found", func(t *testing.T) {
			f := newField[int](cast.ToIntE)
			f.data = data

			result := f.getValuesOr([]interface{}{"qwe"})
			require.Equal(t, uint64(0), result.GetCardinality())
		})
	})
}
