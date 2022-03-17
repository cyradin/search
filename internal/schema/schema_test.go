package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewFromFile(t *testing.T) {
	data := []struct {
		name     string
		src      string
		expected *Schema
	}{
		{
			name: "schema_1",
			src:  "../../test/testdata/schema/schema.json",
			expected: &Schema{
				Fields: []Field{
					{
						Name:     "field_keyword",
						Type:     "keyword",
						Source:   "field_keyword",
						Required: true,
					},
					{
						Name:     "field_slice",
						Type:     "slice",
						Source:   "field_slice",
						Required: false,
						Children: []Field{
							{
								Name:     "field_slice_child_keyword",
								Type:     "keyword",
								Source:   "field_slice_child_keyword",
								Required: true,
							},
							{
								Name:     "field_slice_child_bool",
								Type:     "bool",
								Source:   "field_slice_child_bool",
								Required: true,
							},
						},
					},
				},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result, err := NewFromFile(d.src)
			require.Nil(t, err)
			require.EqualValues(t, d.expected, result)
		})
	}
}
