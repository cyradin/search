package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ValidateDoc(t *testing.T) {
	t.Run("must fail for missing required fields", func(t *testing.T) {
		s := New([]Field{{Type: TypeBool, Required: true, Name: "value"}})
		err := ValidateDoc(s, map[string]interface{}{"value2": true})
		require.Error(t, err)
	})

	t.Run("must not fail for missing not required fields", func(t *testing.T) {
		s := New([]Field{{Type: TypeBool, Required: false, Name: "value"}})
		err := ValidateDoc(s, map[string]interface{}{"value2": true})
		require.Error(t, err)
	})

	t.Run("must fail for extra fields", func(t *testing.T) {
		s := New([]Field{{Type: TypeBool, Required: false, Name: "value"}})
		err := ValidateDoc(s, map[string]interface{}{"value": true, "value2": true})
		require.Error(t, err)
	})

	t.Run("must fail if invalid value type provided", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			s := New([]Field{{Type: TypeBool, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": "true"})
			require.Error(t, err)
		})
		t.Run("keyword", func(t *testing.T) {
			s := New([]Field{{Type: TypeKeyword, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("text", func(t *testing.T) {
			s := New([]Field{{Type: TypeText, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("byte", func(t *testing.T) {
			s := New([]Field{{Type: TypeByte, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("short", func(t *testing.T) {
			s := New([]Field{{Type: TypeShort, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("integer", func(t *testing.T) {
			s := New([]Field{{Type: TypeInteger, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("long", func(t *testing.T) {
			s := New([]Field{{Type: TypeLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("unsigned long", func(t *testing.T) {
			s := New([]Field{{Type: TypeUnsignedLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("float", func(t *testing.T) {
			s := New([]Field{{Type: TypeFloat, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
		t.Run("double", func(t *testing.T) {
			s := New([]Field{{Type: TypeDouble, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.Error(t, err)
		})
	})

	t.Run("must not fail if nil value provided", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			s := New([]Field{{Type: TypeBool, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("keyword", func(t *testing.T) {
			s := New([]Field{{Type: TypeKeyword, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("text", func(t *testing.T) {
			s := New([]Field{{Type: TypeText, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("byte", func(t *testing.T) {
			s := New([]Field{{Type: TypeByte, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("short", func(t *testing.T) {
			s := New([]Field{{Type: TypeShort, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("integer", func(t *testing.T) {
			s := New([]Field{{Type: TypeInteger, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("long", func(t *testing.T) {
			s := New([]Field{{Type: TypeLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("unsigned long", func(t *testing.T) {
			s := New([]Field{{Type: TypeUnsignedLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("float", func(t *testing.T) {
			s := New([]Field{{Type: TypeFloat, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
		t.Run("double", func(t *testing.T) {
			s := New([]Field{{Type: TypeDouble, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": nil})
			require.NoError(t, err)
		})
	})

	t.Run("must not fail if valid value provided", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			s := New([]Field{{Type: TypeBool, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": true})
			require.NoError(t, err)
		})
		t.Run("keyword", func(t *testing.T) {
			s := New([]Field{{Type: TypeKeyword, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": "text"})
			require.NoError(t, err)
		})
		t.Run("text", func(t *testing.T) {
			s := New([]Field{{Type: TypeText, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": "text"})
			require.NoError(t, err)
		})
		t.Run("byte", func(t *testing.T) {
			s := New([]Field{{Type: TypeByte, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("100")})
			require.NoError(t, err)
		})
		t.Run("short", func(t *testing.T) {
			s := New([]Field{{Type: TypeShort, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("1000")})
			require.NoError(t, err)
		})
		t.Run("integer", func(t *testing.T) {
			s := New([]Field{{Type: TypeInteger, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("100000")})
			require.NoError(t, err)
		})
		t.Run("long", func(t *testing.T) {
			s := New([]Field{{Type: TypeLong, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("3000000000")})
			require.NoError(t, err)
		})
		t.Run("unsigned long", func(t *testing.T) {
			s := New([]Field{{Type: TypeUnsignedLong, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("10000000000000000000")})
			require.NoError(t, err)
		})
		t.Run("float", func(t *testing.T) {
			s := New([]Field{{Type: TypeFloat, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("3000000000")})
			require.NoError(t, err)
		})
		t.Run("double", func(t *testing.T) {
			s := New([]Field{{Type: TypeDouble, Required: true, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("3000000000")})
			require.NoError(t, err)
		})
	})

	t.Run("must fail if numeric value is out of range", func(t *testing.T) {
		t.Run("byte min", func(t *testing.T) {
			s := New([]Field{{Type: TypeByte, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-1000")})
			require.Error(t, err)
		})
		t.Run("byte max", func(t *testing.T) {
			s := New([]Field{{Type: TypeByte, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("1000")})
			require.Error(t, err)
		})
		t.Run("short min", func(t *testing.T) {
			s := New([]Field{{Type: TypeShort, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-100000")})
			require.Error(t, err)
		})
		t.Run("short max", func(t *testing.T) {
			s := New([]Field{{Type: TypeShort, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("100000")})
			require.Error(t, err)
		})
		t.Run("integer min", func(t *testing.T) {
			s := New([]Field{{Type: TypeInteger, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-3000000000")})
			require.Error(t, err)
		})
		t.Run("integer max", func(t *testing.T) {
			s := New([]Field{{Type: TypeInteger, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("3000000000")})
			require.Error(t, err)
		})
		t.Run("long min", func(t *testing.T) {
			s := New([]Field{{Type: TypeLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-10000000000000000000")})
			require.Error(t, err)
		})
		t.Run("long max", func(t *testing.T) {
			s := New([]Field{{Type: TypeLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("10000000000000000000")})
			require.Error(t, err)
		})
		t.Run("unsigned long min", func(t *testing.T) {
			s := New([]Field{{Type: TypeUnsignedLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-1")})
			require.Error(t, err)
		})
		t.Run("unsigned long max", func(t *testing.T) {
			s := New([]Field{{Type: TypeUnsignedLong, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("20000000000000000000")})
			require.Error(t, err)
		})
		t.Run("float min", func(t *testing.T) {
			s := New([]Field{{Type: TypeFloat, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-3.4028235E+39")})
			require.Error(t, err)
		})
		t.Run("float max", func(t *testing.T) {
			s := New([]Field{{Type: TypeFloat, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("3.4028235E+39")})
			require.Error(t, err)
		})
		t.Run("double min", func(t *testing.T) {
			s := New([]Field{{Type: TypeDouble, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("-1.7976931348623157e+309")})
			require.Error(t, err)
		})
		t.Run("double max", func(t *testing.T) {
			s := New([]Field{{Type: TypeDouble, Required: false, Name: "value"}})
			err := ValidateDoc(s, map[string]interface{}{"value": json.Number("1.7976931348623157e+309")})
			require.Error(t, err)
		})
	})
}
