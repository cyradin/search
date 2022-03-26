package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Double_AddValue(t *testing.T) {
	var value1 float64 = 1.1
	var value2 float64 = 2.1

	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[float64]uint64
	}{
		{
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testFieldValue{
				{id: 1, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 2, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 2,
			},
		},
		{
			name: "same_id",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 1, value: value2},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
				value2: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 1, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
			},
		},
		{
			name: "different",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 2, value: value2},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
				value2: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewDouble(ctx)

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)

				vv := v.value.(float64)
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

func Test_Double_AddValueSync(t *testing.T) {
	var value1 float64 = 1.1
	var value2 float64 = 2.1

	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[float64]uint64
	}{
		{
			name: "invalid_value_type",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testFieldValue{
				{id: 1, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 2, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 2,
			},
		},
		{
			name: "same_id",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 1, value: value2},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
				value2: 1,
			},
		},
		{
			name: "same_value",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 1, value: value1},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
			},
		},
		{
			name: "different",
			values: []testFieldValue{
				{id: 1, value: value1},
				{id: 2, value: value2},
			},
			expectedCardinality: map[float64]uint64{
				value1: 1,
				value2: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewDouble(ctx)

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

				vv := v.value.(float64)
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
