package field

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Keyword_AddValue(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		expectedCardinality map[string]uint64
		erroneous           bool
	}{
		{
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: 123},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
				{id: 2, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 2,
			},
		},
		{
			name: "same_id",
			values: []testFieldValue{
				{id: 1, value: "value_1"},
				{id: 1, value: "value_2"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 1,
				"value_2": 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "different",
			values: []testFieldValue{
				{id: 1, value: "value_1"},
				{id: 2, value: "value_2"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 1,
				"value_2": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "testdir")
			require.Nil(t, err)
			defer os.RemoveAll(dir)
			testFile := filepath.Join(dir, "file.json")
			ctx := context.Background()
			field, err := NewKeyword(ctx, testFile)

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)

				vv := v.value.(string)
				bm, ok := field.inner.data[vv]
				require.True(t, ok)
				require.True(t, bm.Contains(v.id))
			}

			for k, v := range d.expectedCardinality {
				bm, ok := field.inner.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}

func Test_Keyword_AddValueSync(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		expectedCardinality map[string]uint64
		erroneous           bool
	}{
		{
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: 123},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
				{id: 2, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 2,
			},
		},
		{
			name: "same_id",
			values: []testFieldValue{
				{id: 1, value: "value_1"},
				{id: 1, value: "value_2"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 1,
				"value_2": 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "different",
			values: []testFieldValue{
				{id: 1, value: "value_1"},
				{id: 2, value: "value_2"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 1,
				"value_2": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "testdir")
			require.Nil(t, err)
			defer os.RemoveAll(dir)
			testFile := filepath.Join(dir, "file.json")
			ctx := context.Background()
			field, err := NewKeyword(ctx, testFile)

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

				vv := v.value.(string)
				bm, ok := field.inner.data[vv]
				require.True(t, ok)
				require.True(t, bm.Contains(v.id))
			}

			for k, v := range d.expectedCardinality {
				bm, ok := field.inner.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}
