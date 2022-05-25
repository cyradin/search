package schema

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_ValidateDoc(t *testing.T) {
	data := []struct {
		name   string
		field  Field
		values map[string]interface{}
		ok     bool
	}{
		{
			name:   "required_fail",
			field:  Field{Type: field.TypeBool, Required: true, Name: "value"},
			values: map[string]interface{}{"value2": true},
			ok:     false,
		},
		{
			name:   "allow_missing_fields",
			field:  Field{Type: field.TypeBool, Required: false, Name: "value"},
			values: map[string]interface{}{},
			ok:     true,
		},
		{
			name:   "required_ok",
			field:  Field{Type: field.TypeBool, Required: true, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     true,
		},
		{
			name:   "extra_fields_fail",
			field:  Field{Type: field.TypeBool, Required: true, Name: "value"},
			values: map[string]interface{}{"value": true, "value2": true},
			ok:     false,
		},

		// bool
		{
			name:   "bool_invalid_type",
			field:  Field{Type: field.TypeBool, Required: false, Name: "value"},
			values: map[string]interface{}{"value": "true"},
			ok:     false,
		},
		{
			name:   "bool_allow_nil_value",
			field:  Field{Type: field.TypeBool, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},

		{
			name:   "bool_ok",
			field:  Field{Type: field.TypeBool, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     true,
		},

		// keyword
		{
			name:   "keyword_invalid_type",
			field:  Field{Type: field.TypeKeyword, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "keyword_allow_nil_value",
			field:  Field{Type: field.TypeKeyword, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},

		{
			name:   "keyword_ok",
			field:  Field{Type: field.TypeKeyword, Required: false, Name: "value"},
			values: map[string]interface{}{"value": "value"},
			ok:     true,
		},

		// text
		{
			name:   "text_invalid_type",
			field:  Field{Type: field.TypeText, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "text_allow_nil_value",
			field:  Field{Type: field.TypeText, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},

		{
			name:   "text_ok",
			field:  Field{Type: field.TypeText, Required: false, Name: "value"},
			values: map[string]interface{}{"value": "value"},
			ok:     true,
		},

		// byte
		{
			name:   "byte_invalid_type",
			field:  Field{Type: field.TypeByte, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "byte_allow_nil_value",
			field:  Field{Type: field.TypeByte, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "byte_less_than_min",
			field:  Field{Type: field.TypeByte, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-1000")},
			ok:     false,
		},
		{
			name:   "byte_more_than_max",
			field:  Field{Type: field.TypeByte, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("1000")},
			ok:     false,
		},
		{
			name:   "byte_ok",
			field:  Field{Type: field.TypeByte, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("100")},
			ok:     true,
		},

		// short
		{
			name:   "short_invalid_type",
			field:  Field{Type: field.TypeShort, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "short_allow_nil_value",
			field:  Field{Type: field.TypeShort, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "short_less_than_min",
			field:  Field{Type: field.TypeShort, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-100000")},
			ok:     false,
		},
		{
			name:   "short_more_than_max",
			field:  Field{Type: field.TypeShort, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("100000")},
			ok:     false,
		},
		{
			name:   "short_ok",
			field:  Field{Type: field.TypeShort, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("1000")},
			ok:     true,
		},

		// integer
		{
			name:   "integer_invalid_type",
			field:  Field{Type: field.TypeInteger, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "integer_allow_nil_value",
			field:  Field{Type: field.TypeInteger, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "integer_less_than_min",
			field:  Field{Type: field.TypeInteger, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-3000000000")},
			ok:     false,
		},
		{
			name:   "integer_more_than_max",
			field:  Field{Type: field.TypeInteger, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("3000000000")},
			ok:     false,
		},
		{
			name:   "integer_ok",
			field:  Field{Type: field.TypeInteger, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("100000")},
			ok:     true,
		},

		// long
		{
			name:   "long_invalid_type",
			field:  Field{Type: field.TypeLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "long_allow_nil_value",
			field:  Field{Type: field.TypeLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "long_less_than_min",
			field:  Field{Type: field.TypeLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-10000000000000000000")},
			ok:     false,
		},
		{
			name:   "long_more_than_max",
			field:  Field{Type: field.TypeLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("10000000000000000000")},
			ok:     false,
		},
		{
			name:   "long_ok",
			field:  Field{Type: field.TypeLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("3000000000")},
			ok:     true,
		},

		// unsigned_long
		{
			name:   "unsigned_long_invalid_type",
			field:  Field{Type: field.TypeUnsignedLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "unsigned_long_allow_nil_value",
			field:  Field{Type: field.TypeUnsignedLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "unsigned_long_less_than_min",
			field:  Field{Type: field.TypeUnsignedLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-1")},
			ok:     false,
		},
		{
			name:   "unsigned_long_more_than_max",
			field:  Field{Type: field.TypeUnsignedLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("20000000000000000000")},
			ok:     false,
		},
		{
			name:   "unsigned_long_ok",
			field:  Field{Type: field.TypeUnsignedLong, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("10000000000000000000")},
			ok:     true,
		},

		// float
		{
			name:   "float_invalid_type",
			field:  Field{Type: field.TypeFloat, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "float_allow_nil_value",
			field:  Field{Type: field.TypeFloat, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "float_less_than_min",
			field:  Field{Type: field.TypeFloat, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-3.4028235E+39")},
			ok:     false,
		},
		{
			name:   "float_more_than_max",
			field:  Field{Type: field.TypeFloat, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("3.4028235E+39")},
			ok:     false,
		},
		{
			name:   "float_ok",
			field:  Field{Type: field.TypeFloat, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("3000000000")},
			ok:     true,
		},

		// double
		{
			name:   "double_invalid_type",
			field:  Field{Type: field.TypeDouble, Required: false, Name: "value"},
			values: map[string]interface{}{"value": true},
			ok:     false,
		},
		{
			name:   "double_allow_nil_value",
			field:  Field{Type: field.TypeDouble, Required: false, Name: "value"},
			values: map[string]interface{}{"value": nil},
			ok:     true,
		},
		{
			name:   "double_less_than_min",
			field:  Field{Type: field.TypeDouble, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("-1.7976931348623157e+309")},
			ok:     false,
		},
		{
			name:   "double_more_than_max",
			field:  Field{Type: field.TypeDouble, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("1.7976931348623157e+309")},
			ok:     false,
		},
		{
			name:   "double_ok",
			field:  Field{Type: field.TypeDouble, Required: false, Name: "value"},
			values: map[string]interface{}{"value": json.Number("3000000000")},
			ok:     true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			s := New([]Field{d.field})
			err := ValidateDoc(*s, d.values)

			if err != nil {
				fmt.Println(err.Error())
			}

			if d.ok {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
		})
	}
}
