package schema

import (
	"math"
	"strconv"
	"strings"

	"github.com/cyradin/search/internal/index/field"
	"github.com/go-playground/validator/v10"
	jsoniter "github.com/json-iterator/go"
)

var validationTags = map[field.Type]string{
	field.TypeBool:         "bool",
	field.TypeKeyword:      "string",
	field.TypeText:         "string",
	field.TypeByte:         "byte",
	field.TypeShort:        "short",
	field.TypeInteger:      "integer",
	field.TypeLong:         "long",
	field.TypeUnsignedLong: "unsigned_long",
	field.TypeFloat:        "float",
	field.TypeDouble:       "double",
}

func ValidateDoc(s Schema, source map[string]interface{}) error {
	v := initValidator()
	rules := buildRules(s)

	v.ValidateMap(source, rules)

	return nil
}

func initValidator() *validator.Validate {
	// @todo initialize validator only once, not every time we need to validate document

	v := validator.New()
	v.RegisterValidation("bool", validateBool())
	v.RegisterValidation("string", validateString())

	v.RegisterValidation("byte", validateInt(math.MinInt8, math.MaxInt8))
	v.RegisterValidation("short", validateInt(math.MinInt16, math.MaxInt16))
	v.RegisterValidation("integer", validateInt(math.MinInt32, math.MaxInt32))
	v.RegisterValidation("long", validateInt(math.MinInt64, math.MaxInt64))
	v.RegisterValidation("unsigned_long", validateUint(0, math.MaxUint64))

	v.RegisterValidation("float", validateFloat(-1*math.MaxFloat32, math.MaxFloat32))
	v.RegisterValidation("double", validateFloat(-1*math.MaxFloat64, math.MaxFloat64))

	return v
}

func buildRules(s Schema) map[string]interface{} {
	rules := make(map[string]interface{}, len(s.Fields))
	for _, f := range s.Fields {
		var fRules []string
		if f.Required {
			fRules = append(fRules, "required")
		}

		if tag, ok := validationTags[f.Type]; ok {
			fRules = append(fRules, tag)
		}

		rules[f.Name] = strings.Join(fRules, ",")
	}

	return rules
}

func validateBool() validator.Func {
	return func(fl validator.FieldLevel) bool {
		_, ok := fl.Field().Interface().(bool)
		return ok
	}
}

func validateString() validator.Func {
	return func(fl validator.FieldLevel) bool {
		_, ok := fl.Field().Interface().(string)
		return ok
	}
}

func validateInt(min int64, max int64) validator.Func {
	return func(fl validator.FieldLevel) bool {
		v, ok := fl.Field().Interface().(jsoniter.Number)
		if !ok {
			return false
		}
		vv, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil {
			return false
		}

		if vv > max || vv < min {
			return false
		}

		return true
	}
}

func validateUint(min uint64, max uint64) validator.Func {
	return func(fl validator.FieldLevel) bool {
		v, ok := fl.Field().Interface().(jsoniter.Number)
		if !ok {
			return false
		}
		vv, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return false
		}

		if vv > max || vv < min {
			return false
		}

		return true
	}
}

func validateFloat(min float64, max float64) validator.Func {
	return func(fl validator.FieldLevel) bool {
		v, ok := fl.Field().Interface().(jsoniter.Number)
		if !ok {
			return false
		}
		vv, err := strconv.ParseFloat(v.String(), 64)
		if err != nil {
			return false
		}

		if vv > max || vv < min {
			return false
		}

		return true
	}
}
