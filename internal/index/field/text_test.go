package field

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testAnalyzer1 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		splitter := regexp.MustCompile(`\s`)
		for _, ss := range s {
			result = append(result, splitter.Split(ss, -1)...)
		}
		return next(result)
	}
}

var testAnalyzer2 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		for _, ss := range s {
			result = append(result, ss+"_addition1")
		}
		return next(result)
	}
}

var testAnalyzer3 AnalyzerHandler = func(next Analyzer) Analyzer {
	return func(s []string) []string {
		result := make([]string, 0, len(s))
		for _, ss := range s {
			result = append(result, ss+"_addition2")
		}
		return next(result)
	}
}

func Test_Text_AddValue(t *testing.T) {
	data := []struct {
		name                string
		values              []testFieldValue
		expectedCardinality map[string]uint64
		erroneous           bool
	}{
		{
			name: "one_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value_addition1_addition2": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewText(ctx, "", testAnalyzer2, testAnalyzer3)

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
				bm, ok := field.inner.data[k]
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
			name: "one_value",
			values: []testFieldValue{
				{id: 1, value: "value"},
			},
			expectedCardinality: map[string]uint64{
				"value_addition1_addition2": 1,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			field := NewText(ctx, "", testAnalyzer2, testAnalyzer3)

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
				bm, ok := field.inner.data[k]
				require.True(t, ok)
				require.Equal(t, v, bm.GetCardinality())
			}
		})
	}
}
