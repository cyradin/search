package field

import (
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

func Test_genericField_AddValue(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[bool]uint64
	}{
		{
			name: "one",
			values: []testFieldValue{
				{id: 1, value: true},
			},
			expectedCardinality: map[bool]uint64{
				true: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 2, value: true},
			},
			expectedCardinality: map[bool]uint64{
				true: 2,
			},
		},
		{
			name: "same_id",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 1, value: false},
			},
			expectedCardinality: map[bool]uint64{
				true:  1,
				false: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 1, value: true},
			},
			expectedCardinality: map[bool]uint64{
				true: 1,
			},
		},
		{
			name: "different",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 2, value: false},
			},
			expectedCardinality: map[bool]uint64{
				true:  1,
				false: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			field := newField[bool](cast.ToBoolE)

			for _, v := range d.values {
				field.AddValue(v.id, v.value)
				time.Sleep(time.Millisecond)

				vv := v.value.(bool)
				bm, ok := field.data[vv]
				require.True(t, ok)
				require.True(t, bm.Contains(v.id))
			}

			for k, v := range d.expectedCardinality {
				bm, ok := field.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}

func Test_genericField_getValue(t *testing.T) {
	data := []struct {
		name  string
		data  map[bool]*roaring.Bitmap
		value interface{}
		ok    bool
	}{
		{
			name: "ok",
			data: map[bool]*roaring.Bitmap{
				true: roaring.New(),
			},
			value: true,
			ok:    true,
		},
		{
			name: "not_found",
			data: map[bool]*roaring.Bitmap{
				true: roaring.New(),
			},
			value: false,
			ok:    false,
		},
		{
			name: "invalid_value",
			data: map[bool]*roaring.Bitmap{
				true: roaring.New(),
			},
			value: "qwerty",
			ok:    false,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := newField[bool](cast.ToBoolE)

			f.data = d.data
			f.data[true].Add(1)

			result, ok := f.getValue(d.value)
			if d.ok {
				require.True(t, ok)
				require.NotNil(t, result)

				// check if we return cloned value
				cmap := make(map[bool]uint64)
				for k, v := range f.data {
					cmap[k] = v.GetCardinality()
				}
				result.Add(100)
				for k, v := range f.data {
					require.Equal(t, cmap[k], v.GetCardinality())
				}

				return
			}

			require.False(t, ok)
			require.Nil(t, result)
		})
	}
}

func Test_genericField_getValuesOr(t *testing.T) {
	map1 := roaring.New()
	map1.Add(1)
	map1.Add(2)

	map2 := roaring.New()
	map2.Add(2)
	map2.Add(3)
	map2.Add(4)

	data := []struct {
		name                string
		data                map[int]*roaring.Bitmap
		values              []interface{}
		ok                  bool
		expectedCardinality uint64
	}{
		{
			name: "both_found",
			data: map[int]*roaring.Bitmap{
				0: map1,
				1: map2,
			},
			values:              []interface{}{0, 1, "qwe"},
			ok:                  true,
			expectedCardinality: 4,
		},
		{
			name: "one_found",
			data: map[int]*roaring.Bitmap{
				0: map1,
				1: map2,
			},
			values:              []interface{}{0, 2, "qwe"},
			ok:                  true,
			expectedCardinality: 2,
		},
		{
			name: "none_found",
			data: map[int]*roaring.Bitmap{
				0: map1,
				1: map2,
			},
			values: []interface{}{2, 3, "qwe"},
			ok:     false,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := newField[int](cast.ToIntE)

			f.data = d.data

			result, ok := f.getValuesOr(d.values)
			if d.ok {
				require.True(t, ok)
				require.NotNil(t, result)

				require.Equal(t, d.expectedCardinality, result.GetCardinality())

				// check if we return cloned value
				cmap := make(map[int]uint64)
				for k, v := range f.data {
					cmap[k] = v.GetCardinality()
				}
				result.Add(100)
				for k, v := range f.data {
					require.Equal(t, cmap[k], v.GetCardinality())
				}

				return
			}

			require.False(t, ok)
			require.Nil(t, result)
		})
	}
}
