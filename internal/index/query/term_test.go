package query

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_termQuery(t *testing.T) {
	t.Run("newTermQuery", func(t *testing.T) {
		t.Run("must return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request contains multiple keys", func(t *testing.T) {
			query := `{
				"field1": {},
				"field2": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is empty", func(t *testing.T) {
			query := `{
				"field1": {}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field contains extra keys", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello",
					"qwerty": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request is a valid query", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newTermQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
	})

	t.Run("exec", func(t *testing.T) {
		f := field.NewKeyword()
		f.Add(1, "value")
		ctx := withFields(context.Background(),
			map[string]field.Field{
				"field": f,
			},
		)

		t.Run("must return empty result if field not found", func(t *testing.T) {
			query := `{
				"field1": {
					"query": "value"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.IsEmpty())
		})

		t.Run("must return empty result if value not found", func(t *testing.T) {
			query := `{
				"field": {
					"query": "value1"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.True(t, result.IsEmpty())
		})

		t.Run("must return non-empty result if value is found", func(t *testing.T) {
			query := `{
				"field": {
					"query": "value"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			tq, err := newTermQuery(ctx, req)
			require.NoError(t, err)

			result, err := tq.exec(ctx)
			require.NoError(t, err)
			require.False(t, result.IsEmpty())
			require.ElementsMatch(t, []uint32{1}, result.ToArray())
		})
	})
}

func Test_newTermsQuery(t *testing.T) {
	data := []struct {
		name      string
		req       map[string]interface{}
		erroneous bool
	}{
		{
			name:      "empty",
			req:       map[string]interface{}{},
			erroneous: true,
		},
		{
			name: "multiple_fields",
			req: map[string]interface{}{
				"f1": []string{"1"}, "f2": []string{"2"},
			},
			erroneous: true,
		},
		{
			name: "ok",
			req: map[string]interface{}{
				"f1": []string{"1"},
			},
			erroneous: false,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tq, err := newTermsQuery(context.Background(), d.req)
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
		req         map[string]interface{}
		fieldName   string
		fieldValues map[string][]uint32
		erroneous   bool
		expected    *roaring.Bitmap
	}{
		{
			name: "values_not_an_array",
			req: map[string]interface{}{
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
			req: map[string]interface{}{
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
			req: map[string]interface{}{
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
			req: map[string]interface{}{
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
			f := field.NewKeyword()

			for v, ids := range d.fieldValues {
				for _, id := range ids {
					f.Add(id, v)
				}
			}
			ctx := withFields(context.Background(), map[string]field.Field{d.fieldName: f})

			tq, err := newTermsQuery(ctx, d.req)
			if d.erroneous {
				require.Error(t, err)
				require.Nil(t, tq)
				return
			}

			bm, err := tq.exec(ctx)
			require.NoError(t, err)

			require.NoError(t, err)
			require.Equal(t, d.expected.GetCardinality(), bm.GetCardinality())
		})
	}
}
