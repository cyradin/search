package field

import (
	"fmt"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/schema"
)

var ErrDocNotFound = fmt.Errorf("document not found")

type Index struct {
	name   string
	schema schema.Schema

	fields map[string]Field
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
	fieldsCopy[AllField] = schema.NewField(schema.TypeAll, false, "")

	for name, f := range fieldsCopy {
		fdata := FieldOpts{}

		if f.Analyzer != "" {
			a, err := s.Analyzers[f.Analyzer].Build()
			if err != nil {
				return nil, errs.Errorf("analyzer build err: %w", err)
			}
			fdata.Analyzer = a
		}

		if f.Type == schema.TypeText {
			fdata.Scoring = NewScoring()
		}

		field, err := New(f.Type, fdata)
		if err != nil {
			return nil, errs.Errorf("field build err: %w", err)
		}
		result.fields[name] = field
	}

	return result, nil
}

// Add insert or replace document
func (s *Index) Add(id uint32, source map[string]interface{}) {
	s.fields[AllField].Add(id, true)
	for key, value := range source {
		if f, ok := s.fields[key]; ok {
			f.Add(id, value)
		}
	}
}

func (s *Index) Get(id uint32) (map[string]interface{}, error) {
	if res := s.fields[AllField].Data(id); !res[0].(bool) {
		return nil, ErrDocNotFound
	}

	result := make(map[string]interface{})
	for k, f := range s.fields {
		if k == AllField {
			continue
		}
		result[k] = f.Data(id)
	}
	return result, nil
}

func (s *Index) Delete(id uint32) {
	for _, field := range s.fields {
		field.DeleteDoc(id)
	}
}

func (s *Index) Fields() map[string]Field {
	result := make(map[string]Field)
	for name, f := range s.fields {
		result[name] = f
	}

	return result
}
