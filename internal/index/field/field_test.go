package field

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
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
				{id: 1, value: "true"},
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
			field, err := newGenericField[bool](ctx, "")
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
				{id: 1, value: "true"},
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
			field, err := newGenericField[bool](ctx, "")
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
			f, err := newGenericField[string](context.Background(), d.src)
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

	f1, err := newGenericField[string](context.Background(), src)
	require.Nil(t, err)

	f1.AddValueSync(1, "value")

	err = f1.dump()
	require.Nil(t, err)

	f2, err := newGenericField[string](context.Background(), src)
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
