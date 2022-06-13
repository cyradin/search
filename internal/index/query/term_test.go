package query

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_newTermQuery(t *testing.T) {
	data := []struct {
		name      string
		data      map[string]interface{}
		erroneous bool
	}{
		{
			name:      "empty",
			data:      map[string]interface{}{},
			erroneous: true,
		},
		{
			name: "multiple_fields",
			data: map[string]interface{}{
				"f1": "1", "f2": "2",
			},
			erroneous: true,
		},

		{
			name: "ok",
			data: map[string]interface{}{
				"f1": "1",
			},
			erroneous: false,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tq, err := newTermQuery(queryParams{data: d.data})
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, tq)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tq)
		})
	}
}

func Test_termQuery_exec(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	data := []struct {
		name      string
		data      map[string]interface{}
		fieldName string
		erroneous bool
		expected  *roaring.Bitmap
	}{
		{
			name: "field_not_found",
			data: map[string]interface{}{
				"f2": "1",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "value_not_found",
			data: map[string]interface{}{
				"f1": "2",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "ok",
			data: map[string]interface{}{
				"f1": "1",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  bm,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := field.NewKeyword("")
			f.AddValue(1, "1")

			fields := map[string]field.Field{
				d.fieldName: f,
			}

			tq, err := newTermQuery(queryParams{
				data:   d.data,
				fields: fields,
			})
			require.NoError(t, err)

			bm, err := tq.exec()
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, bm)
				return
			}

			require.NoError(t, err)
			require.Equal(t, d.expected, bm)
		})
	}
}

func Test_newTermsQuery(t *testing.T) {
	data := []struct {
		name      string
		data      map[string]interface{}
		erroneous bool
	}{
		{
			name:      "empty",
			data:      map[string]interface{}{},
			erroneous: true,
		},
		{
			name: "multiple_fields",
			data: map[string]interface{}{
				"f1": []string{"1"}, "f2": []string{"2"},
			},
			erroneous: true,
		},
		{
			name: "ok",
			data: map[string]interface{}{
				"f1": []string{"1"},
			},
			erroneous: false,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tq, err := newTermsQuery(queryParams{data: d.data})
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, tq)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, tq)
		})
	}
}

func Test_execTerms(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)
	bm.Add(2)
	bm.Add(3)

	data := []struct {
		name        string
		data        map[string]interface{}
		fieldName   string
		fieldValues map[string][]uint32
		erroneous   bool
		expected    *roaring.Bitmap
	}{
		{
			name: "values_not_an_array",
			data: map[string]interface{}{
				"val1": "1",
			},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: true,
		},

		{
			name: "field_not_found",
			data: map[string]interface{}{
				"f2": []string{"1"},
			},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "values_not_found",
			data: map[string]interface{}{
				"f1": []string{"3", "4"},
			},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "ok",
			data: map[string]interface{}{
				"f1": []string{"1", "2"},
			},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: false,
			expected:  bm,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f := field.NewKeyword("")

			for v, ids := range d.fieldValues {
				for _, id := range ids {
					f.AddValue(id, v)
				}
			}
			fields := map[string]field.Field{d.fieldName: f}

			tq, err := newTermsQuery(queryParams{data: d.data, fields: fields})
			require.NoError(t, err)

			bm, err := tq.exec()
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, bm)
				return
			}

			require.NoError(t, err)
			require.Equal(t, d.expected.GetCardinality(), bm.GetCardinality())
		})
	}
}
