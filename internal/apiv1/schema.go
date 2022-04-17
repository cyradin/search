package apiv1

import (
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
)

type Schema struct {
	Fields map[string]SchemaField `json:"fields" validate:"required"`
}

type SchemaField struct {
	Type     string                 `json:"type"`
	Required bool                   `json:"required"`
	Fields   map[string]SchemaField `json:"fields"`
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

func (s *Schema) toSchemaField(name string, f SchemaField) schema.Field {
	result := schema.Field{
		Name:     name,
		Type:     field.Type(f.Type),
		Required: f.Required,
	}

	if len(f.Fields) > 0 {
		result.Children = make([]schema.Field, 0, len(f.Fields))
		for name, child := range f.Fields {
			result.Children = append(result.Children, s.toSchemaField(name, child))
		}
	}

	return result
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
