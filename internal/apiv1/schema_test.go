package apiv1

import (
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_Schema_ToSchema(t *testing.T) {
	data := []struct {
		name     string
		src      Schema
		expected schema.Schema
	}{
		{
			name:     "no_fields",
			src:      Schema{},
			expected: schema.Schema{},
		},
		{
			name: "one_field",
			src: Schema{
				Fields: map[string]SchemaField{
					"test": {Type: "text"},
				},
			},
			expected: schema.Schema{
				Fields: []schema.Field{
					{Name: "test", Type: field.TypeText},
				},
			},
		},
		{
			name: "two_fields",
			src: Schema{
				Fields: map[string]SchemaField{
					"test":  {Type: "text"},
					"test2": {Type: "byte"},
				},
			},
			expected: schema.Schema{
				Fields: []schema.Field{
					{Name: "test2", Type: field.TypeByte},
					{Name: "test", Type: field.TypeText},
				},
			},
		},
		{
			name: "nested_fields",
			src: Schema{
				Fields: map[string]SchemaField{
					"test": {
						Type: "slice",
						Fields: map[string]SchemaField{
							"test2": {Type: "byte"},
						},
					},
				},
			},
			expected: schema.Schema{
				Fields: []schema.Field{
					{
						Name: "test",
						Type: field.TypeSlice,
						Children: []schema.Field{
							{Name: "test2", Type: field.TypeByte},
						},
					},
				},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			res := d.src.ToSchema()
			require.ElementsMatch(t, d.expected.Fields, res.Fields)
		})
	}
}

func Test_Schema_FromSchema(t *testing.T) {
	data := []struct {
		name     string
		src      schema.Schema
		expected Schema
	}{
		{
			name:     "no_fields",
			src:      schema.Schema{},
			expected: Schema{Fields: make(map[string]SchemaField)},
		},
		{
			name: "one_field",
			src: schema.Schema{
				Fields: []schema.Field{
					{Name: "test", Type: field.TypeText},
				},
			},
			expected: Schema{
				Fields: map[string]SchemaField{
					"test": {Type: "text"},
				},
			},
		},
		{
			name: "two_fields",
			src: schema.Schema{
				Fields: []schema.Field{
					{Name: "test2", Type: field.TypeByte},
					{Name: "test", Type: field.TypeText},
				},
			},
			expected: Schema{
				Fields: map[string]SchemaField{
					"test":  {Type: "text"},
					"test2": {Type: "byte"},
				},
			},
		},
		{
			name: "nested_fields",
			src: schema.Schema{
				Fields: []schema.Field{
					{
						Name: "test",
						Type: field.TypeSlice,
						Children: []schema.Field{
							{Name: "test2", Type: field.TypeByte},
						},
					},
				},
			},
			expected: Schema{
				Fields: map[string]SchemaField{
					"test": {
						Type: "slice",
						Fields: map[string]SchemaField{
							"test2": {Type: "byte"},
						},
					},
				},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			res := Schema{}
			res.FromSchema(d.src)
			require.EqualValues(t, d.expected, res)
		})
	}
}
