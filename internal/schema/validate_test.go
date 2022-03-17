package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Validate(t *testing.T) {
	data := []struct {
		name   string
		fields []Field
		valid  bool
	}{
		{
			name: "empty_name",
			fields: []Field{
				{Source: "src", Type: Keyword},
			},
			valid: false,
		},
		{
			name: "duplicate_name",
			fields: []Field{
				{Name: "name", Source: "src", Type: Keyword},
				{Name: "name", Source: "src", Type: Keyword},
			},
			valid: false,
		},
		{
			name: "empty_type",
			fields: []Field{
				{Name: "name", Source: "src", Type: Type("invalid")},
			},
			valid: false,
		},
		{
			name: "empty_source",
			fields: []Field{
				{Name: "name", Source: "", Type: Keyword},
			},
			valid: false,
		},
		{
			name: "invalid_type",
			fields: []Field{
				{Name: "name", Source: "src", Type: Type("invalid")},
			},
			valid: false,
		},
		{
			name: "type_cannot_have_child_types",
			fields: []Field{
				{Name: "name", Source: "src", Type: Bool, Children: []Field{
					{Name: "name", Source: "src"},
				}},
			},
			valid: false,
		},
		{
			name: "type_must_have_child_type_defined",
			fields: []Field{
				{Name: "name", Source: "src", Type: Slice},
			},
			valid: false,
		},
		{
			name: "invalid_child",
			fields: []Field{
				{Name: "name", Source: "src", Type: Slice, Children: []Field{
					{Name: "", Source: "src", Type: Bool},
				}},
			},
			valid: false,
		},
		{
			name: "valid",
			fields: []Field{
				{Name: "name", Source: "src", Type: Bool},
				{Name: "name2", Source: "src", Type: Slice, Children: []Field{
					{Name: "name", Source: "src", Type: Keyword},
				}},
			},
			valid: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			s := New(d.fields)
			err := Validate(s)
			if d.valid {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
		})
	}
}
