package index

import (
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_validateValue(t *testing.T) {
	data := []struct {
		name      string
		field     schema.Field
		value     interface{}
		erroneous bool
	}{
		// required
		{
			name:      "required_fail",
			field:     schema.Field{Required: true},
			erroneous: true,
		},

		// bool
		{
			name:  "bool_ok_true",
			field: schema.Field{Required: true, Type: field.TypeBool},
			value: true,
		},
		{
			name:  "bool_ok_false",
			field: schema.Field{Required: true, Type: field.TypeBool},
			value: true,
		},
		{
			name:      "bool_fail",
			field:     schema.Field{Required: true, Type: field.TypeBool},
			value:     "qwerty",
			erroneous: true,
		},

		// keyword
		{
			name:  "keyword_ok",
			field: schema.Field{Required: true, Type: field.TypeKeyword},
			value: "qwerty",
		},
		{
			name:      "keyword_fail",
			field:     schema.Field{Required: true, Type: field.TypeKeyword},
			value:     123,
			erroneous: true,
		},

		// text
		{
			name:  "text_ok",
			field: schema.Field{Required: true, Type: field.TypeText},
			value: "qwerty",
		},
		{
			name:      "text_fail",
			field:     schema.Field{Required: true, Type: field.TypeText},
			value:     123,
			erroneous: true,
		},
		{},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			err := validateValue(d.field, d.value)
			if d.erroneous {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
