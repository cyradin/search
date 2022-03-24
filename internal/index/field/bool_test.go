package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Bool_AddValue(t *testing.T) {
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
			field := NewBool(ctx)

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

func Test_Bool_AddValueSync(t *testing.T) {
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
			field := NewBool(ctx)

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
