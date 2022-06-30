package field

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cyradin/search/internal/index/schema"
)

type Index struct {
	src    string
	schema schema.Schema

	fields map[string]Field
}

func NewIndex(src string, s schema.Schema) (*Index, error) {
	result := &Index{
		src:    src,
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

func (s *Index) load() error {
	return filepath.Walk(s.src, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := strings.TrimRight(info.Name(), fileExt)
		f, ok := s.fields[name]
		if !ok {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("file %q read err: %w", path, err)
		}
		err = f.UnmarshalBinary(data)
		if err != nil {
			return fmt.Errorf("field %q unmarshal err: %w", name, err)
		}

		return nil
	})
}

func (s *Index) dump() error {
	for name, field := range s.fields {
		src := path.Join(s.src, name+fileExt)
		data, err := field.MarshalBinary()
		if err != nil {
			return fmt.Errorf("field %q marshal err: %w", name, err)
		}
		err = os.WriteFile(src, data, filePermissions)
		if err != nil {
			return fmt.Errorf("file %q write err: %w", src, err)
		}
	}

	return nil
}
