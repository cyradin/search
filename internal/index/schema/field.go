package schema

import (
	"fmt"

	"github.com/cyradin/search/internal/index/analyzer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type Type string

const (
	TypeAll  Type = "all"
	TypeBool Type = "bool"

	// String types
	TypeKeyword Type = "keyword"
	TypeText    Type = "text"

	TypeSlice Type = "slice"
	TypeMap   Type = "map"

	// Integer types
	TypeUnsignedLong Type = "unsigned_long" // unsigned int64
	TypeLong         Type = "long"          // signed int64
	TypeInteger      Type = "integer"       // signed int32
	TypeShort        Type = "short"         // signed int16
	TypeByte         Type = "byte"          // signed int8

	// Float types
	TypeDouble Type = "double" // float64
	TypeFloat  Type = "float"  // float32
)

func (t Type) Valid() bool {
	return t == TypeBool ||
		t == TypeKeyword ||
		t == TypeText ||
		t == TypeSlice ||
		t == TypeMap ||
		t == TypeUnsignedLong ||
		t == TypeLong ||
		t == TypeInteger ||
		t == TypeShort ||
		t == TypeByte ||
		t == TypeDouble ||
		t == TypeFloat
}

type Field struct {
	Name      string           `json:"name"`
	Type      Type             `json:"type"`
	Required  bool             `json:"required"`
	Children  map[string]Field `json:"children"`
	Analyzers []analyzer.Type  `json:"analyzers"`
}

func NewField(name string, fieldType Type, required bool, analyzers ...string) Field {
	var typedAnalyzers []analyzer.Type
	if len(analyzers) > 0 {
		typedAnalyzers = make([]analyzer.Type, len(analyzers))
		for i, a := range analyzers {
			typedAnalyzers[i] = analyzer.Type(a)
		}
	}

	return Field{
		Name:      name,
		Type:      fieldType,
		Required:  required,
		Analyzers: typedAnalyzers,
	}
}

func NewFieldWithChildren(name string, fieldType Type, required bool, analyzers []string, children map[string]Field) Field {
	var typedAnalyzers []analyzer.Type
	if len(analyzers) > 0 {
		typedAnalyzers = make([]analyzer.Type, len(analyzers))
		for i, a := range analyzers {
			typedAnalyzers[i] = analyzer.Type(a)
		}
	}

	return Field{
		Name:      name,
		Type:      fieldType,
		Required:  required,
		Analyzers: typedAnalyzers,
		Children:  children,
	}
}

func (f Field) Validate() error {
	return validation.ValidateStruct(&f,
		validation.Field(&f.Name, validation.Required),
		validation.Field(&f.Type, validation.Required, validation.By(validateFieldType())),
		validation.Field(&f.Analyzers, validation.By(validateFieldAnalyzers(f.Type))),
		validation.Field(&f.Children, validation.By(validateFieldChildren(f.Type))),
	)
}

func validateFieldType() validation.RuleFunc {
	return func(value interface{}) error {
		v := value.(Type)
		if !v.Valid() {
			return fmt.Errorf("invalid field type %q", v)
		}
		return nil
	}
}

func validateFieldAnalyzers(t Type) validation.RuleFunc {
	return func(value interface{}) error {
		v := value.([]analyzer.Type)
		if t == TypeText && len(v) == 0 {
			return fmt.Errorf("field has type %q and must have at least one analyzer", t)
		}

		for _, a := range v {
			if !analyzer.Valid(a) {
				return fmt.Errorf("unknown analyzer %q", a)
			}
		}

		return nil
	}
}

func validateFieldChildren(t Type) validation.RuleFunc {
	return func(value interface{}) error {
		if value == nil {
			if t == TypeSlice || t == TypeMap {
				return fmt.Errorf("type %q must have children defined", t)
			}
			return nil
		}
		v := value.(map[string]Field)
		if len(v) == 0 {
			if t == TypeSlice || t == TypeMap {
				return fmt.Errorf("type %q must have children defined", t)
			}
			return nil
		}

		if len(v) != 0 && t != TypeSlice && t != TypeMap {
			return fmt.Errorf("type %q cannot have children fields", t)
		}

		return nil
	}
}
