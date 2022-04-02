package index

import (
	"fmt"
	"reflect"

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

var validateDoc = func(schema *schema.Schema, source map[string]interface{}) error {
	err := validateFields(schema.Fields, source)
	if err != nil {
		return nil
	}

	return nil
}

var validateFields = func(fields []schema.Field, source map[string]interface{}) []error {
	var errors []error
	for _, field := range fields {
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

	return nil
}

var validateValue = func(f schema.Field, value interface{}) error {
	if f.Required && value == nil {
		return fmt.Errorf("field %q required but not defined", f.Name)
	}

	switch f.Type {
	case field.TypeBool:
		if v, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %s", reflect.TypeOf(v))
		}
	case field.TypeKeyword, field.TypeText:
		if v, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %s", reflect.TypeOf(v))
		}
	case field.TypeByte:
		// @todo
	case field.TypeShort:
		// @todo
	case field.TypeInteger:
		// @todo
	case field.TypeLong:
		// @todo
	case field.TypeUnsignedLong:
		// @todo
	case field.TypeFloat:
		// @todo
	case field.TypeDouble:
		// @todo
	}

	// @todo validate
	return nil
}
