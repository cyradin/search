package field

import (
	"context"
	"os"
	"path/filepath"
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
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: "qwe"},
			},
			erroneous: true,
		},
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
			ctx := context.Background()
			field, err := newGenericField[bool](ctx, "", cast.ToBoolE)
			require.Nil(t, err)

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
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

func Test_genericField_AddValueSync(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		expectedCardinality map[bool]uint64
		erroneous           bool
	}{
		{
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: "qwe"},
			},
			erroneous: true,
		},
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
			ctx := context.Background()
			field, err := newGenericField[bool](ctx, "", cast.ToBoolE)
			require.Nil(t, err)

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

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

func Test_genericField_load(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	data := []struct {
		name      string
		src       string
		erroneous bool
		expected  map[string]*roaring.Bitmap
	}{
		{
			name:      "file_not_exists",
			src:       "not_exists",
			erroneous: false,
			expected:  make(map[string]*roaring.Bitmap),
		},
		{
			name:      "ok",
			src:       "../../../test/testdata/field/test.gob",
			erroneous: false,
			expected: map[string]*roaring.Bitmap{
				"value": bm,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f, err := newGenericField[string](context.Background(), d.src, cast.ToStringE)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)

			var expectedKeys []string
			for k := range d.expected {
				expectedKeys = append(expectedKeys, k)
			}
			var dataKeys []string
			for k := range f.data {
				dataKeys = append(dataKeys, k)
			}
			require.EqualValues(t, expectedKeys, dataKeys)

			for k, v := range d.expected {
				require.True(t, f.data[k].Equals(v))
			}
		})
	}
}

func Test_genericField_dump(t *testing.T) {
	dir, err := os.MkdirTemp("", "testdir")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	src := filepath.Join(dir, "data.gob")

	f1, err := newGenericField[string](context.Background(), src, cast.ToStringE)
	require.Nil(t, err)

	f1.AddValueSync(1, "value")

	err = f1.dump()
	require.Nil(t, err)

	f2, err := newGenericField[string](context.Background(), src, cast.ToStringE)
	require.Nil(t, err)

	var expectedKeys []string
	for k := range f1.data {
		expectedKeys = append(expectedKeys, k)
	}
	var dataKeys []string
	for k := range f2.data {
		dataKeys = append(dataKeys, k)
	}
	require.EqualValues(t, expectedKeys, dataKeys)

	for k, v := range f1.data {
		require.True(t, f2.data[k].Equals(v))
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
			f, err := newGenericField[bool](context.Background(), "", cast.ToBoolE)
			require.Nil(t, err)

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
			f, err := newGenericField[int](context.Background(), "", cast.ToIntE)
			require.Nil(t, err)

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
