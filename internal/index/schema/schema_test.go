package schema

import (
	"testing"

	"github.com/cyradin/search/internal/index/analyzer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

// func Test_NewFromFile(t *testing.T) {
// 	data := []struct {
// 		name     string
// 		src      string
// 		expected *Schema
// 	}{
// 		{
// 			name: "schema_1",
// 			src:  "../../../test/testdata/schema/schema.json",
// 			expected: &Schema{
// 				Fields: map[string]Field{
// 					{
// 						Name:     "field_keyword",
// 						Type:     "keyword",
// 						Required: true,
// 					},
// 					{
// 						Name:     "field_slice",
// 						Type:     "slice",
// 						Required: false,
// 						Children: map[string]Field{
// 							{
// 								Name:     "field_slice_child_keyword",
// 								Type:     "keyword",
// 								Required: true,
// 							},
// 							{
// 								Name:     "field_slice_child_bool",
// 								Type:     "bool",
// 								Required: true,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	for _, d := range data {
// 		t.Run(d.name, func(t *testing.T) {
// 			result, err := NewFromFile(d.src)
// 			require.NoError(t, err)
// 			require.EqualValues(t, d.expected, result)
// 		})
// 	}
// }

func Test_Schema_Validate(t *testing.T) {
	t.Run("must fail if field name is empty", func(t *testing.T) {
		s := New(map[string]Field{
			"": {Type: TypeKeyword},
		})
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is empty", func(t *testing.T) {
		s := New(map[string]Field{
			"name": {Name: "name"},
		})
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is invalid", func(t *testing.T) {
		s := New(map[string]Field{
			"name": {Name: "name", Type: "invalid"},
		})
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type cannot have child types", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeBool, Children: map[string]Field{
					"name": Field{Name: "name"},
				}},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type must have children but there aren't any", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeSlice},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field child validation fails", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeSlice, Children: map[string]Field{
					"": {Name: "", Type: TypeBool},
				}},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has no analyzers", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has invalid analyzer", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText, Analyzers: []analyzer.Type{"invalid"}},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must not fail for vaild fields", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name":  {Name: "name", Type: TypeBool},
				"name2": {Name: "name2", Type: TypeText, Analyzers: []analyzer.Type{analyzer.Nop}},
				"name3": {Name: "name3", Type: TypeSlice, Children: map[string]Field{
					"name": {Name: "name", Type: TypeKeyword},
				}},
			},
		)
		err := validation.Validate(s)
		require.NoError(t, err)
	})
}
