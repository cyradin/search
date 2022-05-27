package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_All_AddValue(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		erroneous           bool
		expectedCardinality map[bool]uint64
	}{
		{
			name: "ok",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 1, value: 1},
			},
			expectedCardinality: map[bool]uint64{
				true: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewAll(ctx, "")

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)

				bm, ok := field.inner.data[true]
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

func Test_All_AddValueSync(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		expectedCardinality map[bool]uint64
		erroneous           bool
	}{
		{
			name: "ok",
			values: []testFieldValue{
				{id: 1, value: true},
				{id: 1, value: 1},
			},
			expectedCardinality: map[bool]uint64{
				true: 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewAll(ctx, "")

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}

				bm, ok := field.inner.data[true]
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
