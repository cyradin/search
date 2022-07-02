package field

import (
	"fmt"

	"github.com/cyradin/search/internal/index/schema"
)

type Index struct {
	name   string
	schema schema.Schema

	fields    map[string]Field
	relevance map[string]*Relevance
}

func NewIndex(name string, s schema.Schema) (*Index, error) {
	result := &Index{
		name:   name,
		schema: s,
		fields: make(map[string]Field),
	}

	// add "allField" which contains all documents
	fieldsCopy := make(map[string]schema.Field)
	for name, field := range s.Fields {
		fieldsCopy[name] = field
	}
	fieldsCopy[AllField] = schema.NewField(AllField, schema.TypeAll, false, "")

	for _, f := range fieldsCopy {
		fdata := FieldData{Type: f.Type}

		if f.Analyzer != "" {
			a, err := s.Analyzers[f.Analyzer].Build()
			if err != nil {
				return nil, fmt.Errorf("analyzer build err: %w", err)
			}
			fdata.Analyzer = a
		}

		field, err := New(fdata)
		if err != nil {
			return nil, fmt.Errorf("field build err: %w", err)
		}
		result.fields[f.Name] = field
	}

	return result, nil
}

func (s *Index) Add(id uint32, source map[string]interface{}) {
	for key, value := range source {
		if f, ok := s.fields[key]; ok {
			f.AddValue(id, value)
			s.fields[AllField].AddValue(id, value)
		}
	}
}

func (s *Index) Fields() map[string]Field {
	result := make(map[string]Field)
	for name, f := range s.fields {
		result[name] = f
	}

	return result
}
