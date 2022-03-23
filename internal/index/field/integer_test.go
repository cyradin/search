package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testIntegerValue struct {
	id    uint32
	value interface{}
}

func Test_Integer_AddValue(t *testing.T) {
	var value1 int32 = 1
	var value2 int32 = 2

	data := []struct {
		name                string
		values              []testIntegerValue
		erroneous           bool
		expectedCardinality map[int32]uint64
	}{
		{
			name: "invalid_value_type",
			values: []testIntegerValue{
				{id: 1, value: "value"},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testIntegerValue{
				{id: 1, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
			},
		},
		{
			name: "same_value",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 2, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 2,
			},
		},
		{
			name: "same_id",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 1, value: value2},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
				value2: 1,
			},
		},
		{
			name: "same_value",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 1, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
			},
		},
		{
			name: "different",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 2, value: value2},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
				value2: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewInteger(ctx)

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)

				vv := v.value.(int32)
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

func Test_Integer_AddValueSync(t *testing.T) {
	var value1 int32 = 1
	var value2 int32 = 2

	data := []struct {
		name                string
		values              []testIntegerValue
		erroneous           bool
		expectedCardinality map[int32]uint64
	}{
		{
			name: "invalid_value_type",
			values: []testIntegerValue{
				{id: 1, value: 123},
			},
			erroneous: true,
		},
		{
			name: "one",
			values: []testIntegerValue{
				{id: 1, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
			},
		},
		{
			name: "same_value",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 2, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 2,
			},
		},
		{
			name: "same_id",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 1, value: value2},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
				value2: 1,
			},
		},
		{
			name: "same_value",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 1, value: value1},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
			},
		},
		{
			name: "different",
			values: []testIntegerValue{
				{id: 1, value: value1},
				{id: 2, value: value2},
			},
			expectedCardinality: map[int32]uint64{
				value1: 1,
				value2: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewInteger(ctx)

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

				vv := v.value.(int32)
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
