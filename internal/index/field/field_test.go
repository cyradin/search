package field

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

func Test_genericField(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		t.Run("can add one value", func(t *testing.T) {
			field := newField[bool]()
			field.Add(1, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.Equal(t, uint64(1), bm.GetCardinality())
		})
		t.Run("can add two two different ids with different values", func(t *testing.T) {
			field := newField[bool]()
			field.Add(1, true)
			field.Add(2, false)
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
			field := newField[bool]()
			field.Add(1, true)
			field.Add(2, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.True(t, bm.Contains(2))
			require.Equal(t, uint64(2), bm.GetCardinality())
		})
		t.Run("can overwrite value", func(t *testing.T) {
			field := newField[bool]()
			field.Add(1, true)
			field.Add(1, true)
			bm, ok := field.data[true]
			require.True(t, ok)
			require.True(t, bm.Contains(1))
			require.Equal(t, uint64(1), bm.GetCardinality())
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("can return value if found", func(t *testing.T) {
			f := newField[bool]()
			f.data = map[bool]*roaring.Bitmap{
				true: roaring.New(),
			}
			f.data[true].Add(1)

			result := f.Get(true)
			require.True(t, result.Contains(1))
			require.Equal(t, uint64(1), result.GetCardinality())
		})
		t.Run("can return empty bitmap if not found", func(t *testing.T) {
			f := newField[bool]()
			f.data = map[bool]*roaring.Bitmap{
				true: roaring.New(),
			}
			f.data[true].Add(1)

			result := f.Get(false)
			require.False(t, result.Contains(1))
			require.Equal(t, uint64(0), result.GetCardinality())
		})
	})

	t.Run("GetOr", func(t *testing.T) {
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
			f := newField[int]()
			f.data = data

			result := f.GetOr([]int{0, 1})
			require.Equal(t, uint64(4), result.GetCardinality())
			require.True(t, result.Contains(1))
			require.True(t, result.Contains(2))
			require.True(t, result.Contains(3))
			require.True(t, result.Contains(4))
		})
		t.Run("can return non-empty result if at least one value found", func(t *testing.T) {
			f := newField[int]()
			f.data = data

			result := f.GetOr([]int{0, 100})
			require.Equal(t, uint64(2), result.GetCardinality())
			require.True(t, result.Contains(1))
			require.True(t, result.Contains(2))
		})
		t.Run("can return empty result if nothing found", func(t *testing.T) {
			f := newField[int]()
			f.data = data

			result := f.GetOr([]int{100})
			require.Equal(t, uint64(0), result.GetCardinality())
		})
	})

	t.Run("GetAnd", func(t *testing.T) {
		map1 := roaring.New()
		map1.Add(1)
		map1.Add(2)

		map2 := roaring.New()
		map2.Add(2)
		map2.Add(3)
		map2.Add(4)

		map3 := roaring.New()
		map3.Add(1)

		data := map[int]*roaring.Bitmap{
			0: map1,
			1: map2,
			2: map3,
		}

		t.Run("can return intersection if both values found", func(t *testing.T) {
			f := newField[int]()
			f.data = data

			result := f.GetAnd([]int{0, 1})
			require.Equal(t, uint64(1), result.GetCardinality())
			require.True(t, result.Contains(2))
		})
		t.Run("can return empty result if no intersection exists between values", func(t *testing.T) {
			f := newField[int]()
			f.data = data

			result := f.GetAnd([]int{1, 2})
			require.Equal(t, uint64(0), result.GetCardinality())
		})
		t.Run("can return empty result if nothing found", func(t *testing.T) {
			f := newField[int]()
			f.data = data

			result := f.GetAnd([]int{100})
			require.Equal(t, uint64(0), result.GetCardinality())
		})
	})
}
