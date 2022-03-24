package field

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testAnalyzer Analyzer = func(s []string) []string {
	result := make([]string, 0, len(s))
	splitter := regexp.MustCompile(`\s`)
	for _, ss := range s {
		result = append(result, splitter.Split(ss, -1)...)
	}
	return result
}

func Test_Text_AddValue(t *testing.T) {
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
			name: "one_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "one_value_two_tokens",
			values: []testFieldValue{
				{id: 1, value: "value1 value2"},
			},
			expectedCardinality: map[string]uint64{
				"value1": 1,
				"value2": 1,
			},
		},
		{
			name: "two_values_same_value",
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
				{id: 1, value: "value_1 value_2"},
				{id: 2, value: "value_2 value_1"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 2,
				"value_2": 2,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewText(ctx, testAnalyzer)

			for _, v := range d.values {
				err := field.AddValue(v.id, v.value)
				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
				time.Sleep(time.Millisecond)
			}

			for k, v := range d.expectedCardinality {
				bm, ok := field.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}

func Test_Text_AddValueSync(t *testing.T) {
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
			name: "one_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value": 1,
			},
		},
		{
			name: "one_value_two_tokens",
			values: []testFieldValue{
				{id: 1, value: "value1 value2"},
			},
			expectedCardinality: map[string]uint64{
				"value1": 1,
				"value2": 1,
			},
		},
		{
			name: "two_values_same_value",
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
				{id: 1, value: "value_1 value_2"},
				{id: 2, value: "value_2 value_1"},
			},
			expectedCardinality: map[string]uint64{
				"value_1": 2,
				"value_2": 2,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewText(ctx, testAnalyzer)

			for _, v := range d.values {
				err := field.AddValueSync(v.id, v.value)

				if d.erroneous {
					require.NotNil(t, err)
					continue
				} else {
					require.Nil(t, err)
				}
			}

			for k, v := range d.expectedCardinality {
				bm, ok := field.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}
