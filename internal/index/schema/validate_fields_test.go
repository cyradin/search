package schema

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func Test_validateString(t *testing.T) {
	data := []struct {
		name  string
		tag   string
		value interface{}
		ok    bool
	}{
		// string
		{
			name:  "invalid_type",
			tag:   "string",
			value: true,
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "string",
			value: "qwerty",
			ok:    true,
		},
	}

	for _, d := range data {
		t.Run(d.name+"_"+d.tag, func(t *testing.T) {
			v := initValidator()
			errors := v.Var(d.value, d.tag)
			if d.ok {
				require.Nil(t, errors)
				return
			}
			require.NotNil(t, errors)
		})
	}
}

func Test_validateBool(t *testing.T) {
	data := []struct {
		name  string
		tag   string
		value interface{}
		ok    bool
	}{
		// bool
		{
			name:  "invalid_type",
			tag:   "bool",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "bool",
			value: true,
			ok:    true,
		},
	}

	for _, d := range data {
		t.Run(d.name+"_"+d.tag, func(t *testing.T) {
			v := initValidator()
			errors := v.Var(d.value, d.tag)
			if d.ok {
				require.Nil(t, errors)
				return
			}
			require.NotNil(t, errors)
		})
	}
}

func Test_validateInt(t *testing.T) {
	data := []struct {
		name  string
		tag   string
		value interface{}
		ok    bool
	}{
		// byte
		{
			name:  "invalid_type",
			tag:   "byte",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "byte",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "byte",
			value: jsoniter.Number("-1000"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "byte",
			value: jsoniter.Number("1000"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "byte",
			value: jsoniter.Number("100"),
			ok:    true,
		},
		// short
		{
			name:  "invalid_type",
			tag:   "short",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "short",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "short",
			value: jsoniter.Number("-100000"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "short",
			value: jsoniter.Number("100000"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "short",
			value: jsoniter.Number("100"),
			ok:    true,
		},
		// integer
		{
			name:  "invalid_type",
			tag:   "integer",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "integer",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "integer",
			value: jsoniter.Number("-3000000000"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "integer",
			value: jsoniter.Number("3000000000"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "integer",
			value: jsoniter.Number("100"),
			ok:    true,
		},
		// long
		{
			name:  "invalid_type",
			tag:   "long",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "long",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "long",
			value: jsoniter.Number("-10000000000000000000"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "long",
			value: jsoniter.Number("10000000000000000000"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "long",
			value: jsoniter.Number("100"),
			ok:    true,
		},
	}

	for _, d := range data {
		t.Run(d.name+"_"+d.tag, func(t *testing.T) {
			v := initValidator()
			errors := v.Var(d.value, d.tag)
			if d.ok {
				require.Nil(t, errors)
				return
			}
			require.NotNil(t, errors)
		})
	}
}

func Test_validateUint(t *testing.T) {
	data := []struct {
		name  string
		tag   string
		value interface{}
		ok    bool
	}{
		// unsigned_long
		{
			name:  "invalid_type",
			tag:   "unsigned_long",
			value: "qwerty",
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "unsigned_long",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "unsigned_long",
			value: jsoniter.Number("-1"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "unsigned_long",
			value: jsoniter.Number("20000000000000000000"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "unsigned_long",
			value: jsoniter.Number("1000000"),
			ok:    true,
		},
	}

	for _, d := range data {
		t.Run(d.name+"_"+d.tag, func(t *testing.T) {
			v := initValidator()
			errors := v.Var(d.value, d.tag)
			if d.ok {
				require.Nil(t, errors)
				return
			}
			require.NotNil(t, errors)
		})
	}
}

func Test_validateFloat(t *testing.T) {
	data := []struct {
		name  string
		tag   string
		value interface{}
		ok    bool
	}{
		// float
		{
			name:  "invalid_type",
			tag:   "float",
			value: 123,
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "float",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "float",
			value: jsoniter.Number("-3.4028235E+39"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "float",
			value: jsoniter.Number("3.4028235E+39"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "float",
			value: jsoniter.Number("1000000"),
			ok:    true,
		},
		// double
		{
			name:  "invalid_type",
			tag:   "double",
			value: 123,
			ok:    false,
		},
		{
			name:  "parse_error",
			tag:   "double",
			value: jsoniter.Number("qwerty"),
			ok:    false,
		},
		{
			name:  "less_than_min",
			tag:   "double",
			value: jsoniter.Number("-1.7976931348623157e+309"),
			ok:    false,
		},
		{
			name:  "more_than_max",
			tag:   "double",
			value: jsoniter.Number("1.7976931348623157e+309"),
			ok:    false,
		},
		{
			name:  "ok",
			tag:   "double",
			value: jsoniter.Number("1000000"),
			ok:    true,
		},
	}

	for _, d := range data {
		t.Run(d.name+"_"+d.tag, func(t *testing.T) {
			v := initValidator()
			errors := v.Var(d.value, d.tag)
			if d.ok {
				require.Nil(t, errors)
				return
			}
			require.NotNil(t, errors)
		})
	}
}
