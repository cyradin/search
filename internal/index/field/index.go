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

type index struct {
	src    string
	schema schema.Schema

	fields map[string]Field
}

func NewIndex(src string, s schema.Schema) (*index, error) {
	result := &index{
		src:    src,
		schema: s,
		fields: make(map[string]Field),
	}

	// add "allField" which contains all documents
	fields := make([]schema.Field, len(s.Fields))
	copy(fields, s.Fields)
	fields = append(fields, schema.Field{
		Name:     AllField,
		Required: false,
		Type:     schema.TypeAll,
	})

	for _, f := range fields {
		field, err := New(f.Type)
		if err != nil {
			return nil, err
		}
		result.fields[f.Name] = field
	}

	err := result.load()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *index) Add(id uint32, source map[string]interface{}) {
	for key, value := range source {
		if f, ok := s.fields[key]; ok {
			f.AddValue(id, value)
			s.fields[AllField].AddValue(id, value)
		}
	}
}

func (s *index) Fields() map[string]Field {
	result := make(map[string]Field)
	for name, f := range s.fields {
		result[name] = f
	}

	return result
}

func (s *index) load() error {
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

func (s *index) dump() error {
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
