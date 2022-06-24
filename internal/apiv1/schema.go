package apiv1

import (
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type Schema struct {
	Fields map[string]SchemaField `json:"fields"`
}

type SchemaField struct {
	Type      string                 `json:"type"`
	Required  bool                   `json:"required"`
	Analyzers []string               `json:"analyzers"`
	Fields    map[string]SchemaField `json:"fields,omitempty"`
}

func (s *Schema) ToSchema() schema.Schema {
	res := schema.Schema{}

	if len(s.Fields) != 0 {
		res.Fields = make([]schema.Field, 0, len(s.Fields))
		for name, f := range s.Fields {
			res.Fields = append(res.Fields, s.toSchemaField(name, f))
		}
	}

	return res
}

func (s Schema) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Fields, validation.Required),
	)
}

func (s *Schema) toSchemaField(name string, f SchemaField) schema.Field {
	children := make([]schema.Field, 0, len(f.Fields))
	for name, child := range f.Fields {
		children = append(children, s.toSchemaField(name, child))
	}

	if len(children) > 0 {
		return schema.NewFieldWithChildren(name, schema.Type(f.Type), f.Required, f.Analyzers, children...)

	}

	return schema.NewField(name, schema.Type(f.Type), f.Required, f.Analyzers...)
}

func (s *Schema) FromSchema(src schema.Schema) {
	s.Fields = make(map[string]SchemaField)
	for _, f := range src.Fields {
		s.Fields[f.Name] = s.fromSchemaField(f)
	}
}

func (s *Schema) fromSchemaField(f schema.Field) SchemaField {
	result := SchemaField{
		Type:     string(f.Type),
		Required: f.Required,
	}

	if len(f.Children) > 0 {
		result.Fields = make(map[string]SchemaField)
		for _, child := range f.Children {
			result.Fields[child.Name] = s.fromSchemaField(child)
		}
	}

	return result
}
