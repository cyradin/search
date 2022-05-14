package query

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

type testFieldValue struct {
	values map[interface{}]*roaring.Bitmap
}

func (f *testFieldValue) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	if v, ok := f.values[value]; ok {
		return v, true
	}

	return nil, false
}

func Test_term(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	data := []struct {
		name      string
		data      map[string]interface{}
		fields    map[string]fieldValue
		erroneous bool
		expected  *roaring.Bitmap
	}{
		{
			name:      "empty",
			data:      map[string]interface{}{},
			fields:    nil,
			erroneous: true,
		},
		{
			name: "multiple_fields",
			data: map[string]interface{}{
				"val1": "1", "val2": "2",
			},
			fields:    nil,
			erroneous: true,
		},
		{
			name: "field_not_found",
			data: map[string]interface{}{
				"val1": "1",
			},
			fields: map[string]fieldValue{
				"val2": &testFieldValue{},
			},
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "value_not_found",
			data: map[string]interface{}{
				"val1": "1",
			},
			fields: map[string]fieldValue{
				"val1": &testFieldValue{
					values: map[interface{}]*roaring.Bitmap{
						"2": roaring.New(),
					},
				},
			},
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "ok",
			data: map[string]interface{}{
				"val1": "1",
			},
			fields: map[string]fieldValue{
				"val1": &testFieldValue{
					values: map[interface{}]*roaring.Bitmap{
						"1": bm,
					},
				},
			},
			erroneous: false,
			expected:  bm,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			bm, err := term(d.data, d.fields)
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, bm)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, bm)
		})
	}
}
