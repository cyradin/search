package index

import (
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	jsoniter "github.com/json-iterator/go"
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
			name:      "required_type_error",
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
			name:      "bool_type_error",
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
			name:      "keyword_type_error",
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
			name:      "text_type_error",
			field:     schema.Field{Required: true, Type: field.TypeText},
			value:     123,
			erroneous: true,
		},

		// byte
		{
			name:  "byte_ok",
			field: schema.Field{Required: true, Type: field.TypeByte},
			value: jsoniter.Number("123"),
		},
		{
			name:      "byte_type_error",
			field:     schema.Field{Required: true, Type: field.TypeByte},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "byte_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeByte},
			value:     jsoniter.Number("-1000"),
			erroneous: true,
		},
		{
			name:      "byte_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeByte},
			value:     jsoniter.Number("1000"),
			erroneous: true,
		},

		// short
		{
			name:  "short_ok",
			field: schema.Field{Required: true, Type: field.TypeShort},
			value: jsoniter.Number("30000"),
		},
		{
			name:      "short_type_error",
			field:     schema.Field{Required: true, Type: field.TypeShort},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "short_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeShort},
			value:     jsoniter.Number("-100000"),
			erroneous: true,
		},
		{
			name:      "short_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeShort},
			value:     jsoniter.Number("100000"),
			erroneous: true,
		},

		// integer
		{
			name:  "integer_ok",
			field: schema.Field{Required: true, Type: field.TypeInteger},
			value: jsoniter.Number("100000"),
		},
		{
			name:      "integer_type_error",
			field:     schema.Field{Required: true, Type: field.TypeInteger},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "integer_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeInteger},
			value:     jsoniter.Number("-3000000000"),
			erroneous: true,
		},
		{
			name:      "integer_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeInteger},
			value:     jsoniter.Number("3000000000"),
			erroneous: true,
		},

		// long
		{
			name:  "long_ok",
			field: schema.Field{Required: true, Type: field.TypeLong},
			value: jsoniter.Number("3000000000"),
		},
		{
			name:      "long_type_error",
			field:     schema.Field{Required: true, Type: field.TypeLong},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "long_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeLong},
			value:     jsoniter.Number("-10000000000000000000"),
			erroneous: true,
		},
		{
			name:      "long_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeLong},
			value:     jsoniter.Number("10000000000000000000"),
			erroneous: true,
		},

		// unsigned_long
		{
			name:  "unsigned_long_ok",
			field: schema.Field{Required: true, Type: field.TypeUnsignedLong},
			value: jsoniter.Number("10000000000000000000"),
		},
		{
			name:      "unsigned_long_type_error",
			field:     schema.Field{Required: true, Type: field.TypeUnsignedLong},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "unsigned_long_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeUnsignedLong},
			value:     jsoniter.Number("-1"),
			erroneous: true,
		},
		{
			name:      "unsigned_long_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeUnsignedLong},
			value:     jsoniter.Number("20000000000000000000"),
			erroneous: true,
		},

		// float
		{
			name:  "float_ok",
			field: schema.Field{Required: true, Type: field.TypeFloat},
			value: jsoniter.Number("1000000"),
		},
		{
			name:      "float_type_error",
			field:     schema.Field{Required: true, Type: field.TypeFloat},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "float_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeFloat},
			value:     jsoniter.Number("-3.4028235E+39"),
			erroneous: true,
		},
		{
			name:      "float_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeFloat},
			value:     jsoniter.Number("3.4028235E+39"),
			erroneous: true,
		},

		// double
		{
			name:  "double_ok",
			field: schema.Field{Required: true, Type: field.TypeDouble},
			value: jsoniter.Number("1.7976931348623157e+308"),
		},
		{
			name:      "double_type_error",
			field:     schema.Field{Required: true, Type: field.TypeDouble},
			value:     "qwerty",
			erroneous: true,
		},
		{
			name:      "double_out_of_range_min",
			field:     schema.Field{Required: true, Type: field.TypeDouble},
			value:     jsoniter.Number("-1.7976931348623157e+309"),
			erroneous: true,
		},
		{
			name:      "double_out_of_range_max",
			field:     schema.Field{Required: true, Type: field.TypeDouble},
			value:     jsoniter.Number("1.7976931348623157e+309"),
			erroneous: true,
		},
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
