package field

import (
	"context"
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
			name: "one",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewKeyword(ctx, "")

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
			name: "one",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewKeyword(ctx, "")

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
