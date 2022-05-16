package query

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_term(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	data := []struct {
		name      string
		data      entity.Query
		fieldName string
		erroneous bool
		expected  *roaring.Bitmap
	}{
		{
			name:      "empty",
			data:      entity.Query{},
			fieldName: "f1",
			erroneous: true,
		},
		{
			name: "multiple_fields",
			data: entity.Query{
				"f1": "1", "f2": "2",
			},
			fieldName: "f1",
			erroneous: true,
		},
		{
			name: "field_not_found",
			data: entity.Query{
				"f2": "1",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "value_not_found",
			data: entity.Query{
				"f1": "2",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  roaring.New(),
		},
		{
			name: "ok",
			data: entity.Query{
				"f1": "1",
			},
			fieldName: "f1",
			erroneous: false,
			expected:  bm,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			f, err := field.NewKeyword(context.Background(), "")
			require.Nil(t, err)

			err = f.AddValueSync(1, "1")
			require.Nil(t, err)

			fields := map[string]fieldValue{
				d.fieldName: f,
			}

			bm, err := execTerm(d.data, fields, "")
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

func Test_execTerms(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)
	bm.Add(2)
	bm.Add(3)

	data := []struct {
		name        string
		data        entity.Query
		fieldName   string
		fieldValues map[string][]uint32
		erroneous   bool
		expected    *roaring.Bitmap
	}{
		{
			name:      "empty",
			data:      entity.Query{},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: true,
		},
		{
			name: "multiple_fields",
			data: entity.Query{
				"f1": []string{"1"}, "f2": []string{"2"},
			},
			fieldName: "f1",
			fieldValues: map[string][]uint32{
				"1": {1, 2},
				"2": {1, 2, 3},
			},
			erroneous: true,
		},
		{
			name: "values_not_an_array",
			data: entity.Query{
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
			data: entity.Query{
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
			data: entity.Query{
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
			data: entity.Query{
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
			f, err := field.NewKeyword(context.Background(), "")
			require.Nil(t, err)

			for v, ids := range d.fieldValues {
				for _, id := range ids {
					err := f.AddValueSync(id, v)
					require.Nil(t, err)
				}
			}
			fields := map[string]fieldValue{d.fieldName: f}

			bm, err := execTerms(d.data, fields, "")
			if d.erroneous {
				require.NotNil(t, err)
				require.Nil(t, bm)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected.GetCardinality(), bm.GetCardinality())
		})
	}
}
