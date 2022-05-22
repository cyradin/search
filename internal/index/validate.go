package index

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type FieldValidationError struct {
	field  schema.Field
	value  interface{}
	parent error
}

var validateDoc = func(schema schema.Schema, source entity.DocSource) error {
	err := validateFields(schema.Fields, source)
	if err != nil {
		return nil
	}

	return nil
}

var validateFields = func(fields []schema.Field, source entity.DocSource) []error {
	var errors []error

	visited := make(map[string]struct{})
	for _, field := range fields {
		visited[field.Name] = struct{}{}

		value := source[field.Name]
		err := validateValue(field, value)
		if err != nil {
			errors = append(errors, fmt.Errorf("validation err: %w", err))
			continue
		}

		// @todo implement recursive validation for slice and map types
		// if ok && len(field.Children) != 0 {
		// 	err := validateFields(field.Children, value)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
	}

	for name := range source {
		if _, ok := visited[name]; !ok {
			errors = append(errors, fmt.Errorf("validation err: field %q is not defined in index schema", name))
		}
	}

	return errors
}

var validateValue = func(f schema.Field, value interface{}) error {
	if f.Required && value == nil {
		return fmt.Errorf("field %q required but not defined", f.Name)
	}

	if value == nil {
		return nil
	}

	switch f.Type {
	case field.TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %s", reflect.TypeOf(value))
		}
	case field.TypeKeyword, field.TypeText:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %s", reflect.TypeOf(value))
		}
	case field.TypeByte:
		return validateJsonInt(value, math.MinInt8, math.MaxInt8)
	case field.TypeShort:
		return validateJsonInt(value, math.MinInt16, math.MaxInt16)
	case field.TypeInteger:
		return validateJsonInt(value, math.MinInt32, math.MaxInt32)
	case field.TypeLong:
		return validateJsonInt(value, math.MinInt64, math.MaxInt64)
	case field.TypeUnsignedLong:
		return validateJsonUint(value, 0, math.MaxUint64)
	case field.TypeFloat:
		return validateJsonFloat(value, -1*math.MaxFloat32, math.MaxFloat32)
	case field.TypeDouble:
		return validateJsonFloat(value, -1*math.MaxFloat64, math.MaxFloat64)
	}

	// @todo validate
	return nil
}

var validateJsonInt = func(value interface{}, min int64, max int64) error {
	v, ok := value.(jsoniter.Number)
	if !ok {
		return fmt.Errorf("expected json.Number, got %s", reflect.TypeOf(value))
	}
	vv, err := strconv.ParseInt(v.String(), 10, 64)
	if err != nil {
		return err
	}

	if vv > max || vv < min {
		return fmt.Errorf("value %s is out of range", v.String())
	}

	return nil
}

var validateJsonUint = func(value interface{}, min uint64, max uint64) error {
	v, ok := value.(jsoniter.Number)
	if !ok {
		return fmt.Errorf("expected json.Number, got %s", reflect.TypeOf(value))
	}
	vv, err := strconv.ParseUint(v.String(), 10, 64)
	if err != nil {
		return err
	}

	if vv > max || vv < min {
		return fmt.Errorf("value %s is out of range", v.String())
	}

	return nil
}

var validateJsonFloat = func(value interface{}, min float64, max float64) error {
	v, ok := value.(jsoniter.Number)
	if !ok {
		return fmt.Errorf("expected json.Number, got %s", reflect.TypeOf(value))
	}
	vv, err := strconv.ParseFloat(v.String(), 64)
	if err != nil {
		return err
	}

	if vv > max || vv < min {
		return fmt.Errorf("value %s is out of range", v.String())
	}

	return nil
}
