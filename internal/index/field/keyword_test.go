package field

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Keyword_Set(t *testing.T) {
	data := []struct {
		name                string
		values              []keywordValue
		expectedCardinality map[string]uint64
	}{
		{
			name: "one",
			values: []keywordValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "same_value",
			values: []keywordValue{
				{id: 1, value: "value"},
				{id: 2, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 2,
			},
		},
		{
			name: "same_id",
			values: []keywordValue{
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
			values: []keywordValue{
				{id: 1, value: "value"},
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "different",
			values: []keywordValue{
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
			ctx := context.Background()
			field := NewKeyword(ctx)

			for _, v := range d.values {
				field.Set(v.id, v.value)
				time.Sleep(time.Millisecond)
				bm, ok := field.data[v.value]
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

func Test_Keyword_SetSync(t *testing.T) {
	data := []struct {
		name                string
		values              []keywordValue
		expectedCardinality map[string]uint64
	}{
		{
			name: "one",
			values: []keywordValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "same_value",
			values: []keywordValue{
				{id: 1, value: "value"},
				{id: 2, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 2,
			},
		},
		{
			name: "same_id",
			values: []keywordValue{
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
			values: []keywordValue{
				{id: 1, value: "value"},
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "different",
			values: []keywordValue{
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
			ctx := context.Background()
			field := NewKeyword(ctx)

			for _, v := range d.values {
				field.SetSync(v.id, v.value)
				bm, ok := field.data[v.value]
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
