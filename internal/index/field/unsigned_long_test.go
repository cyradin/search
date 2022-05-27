package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_UnsignedLong_AddValue(t *testing.T) {
	var value1 uint64 = 1

	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[uint64]uint64
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
			expectedCardinality: map[uint64]uint64{
				value1: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewUnsignedLong(ctx, "")

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)

				vv := v.value.(uint64)
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

func Test_UnsignedLong_AddValueSync(t *testing.T) {
	var value1 uint64 = 1

	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[uint64]uint64
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
				{id: 1, value: value1},
			},
			expectedCardinality: map[uint64]uint64{
				value1: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewUnsignedLong(ctx, "")

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

				vv := v.value.(uint64)
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
