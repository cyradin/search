package index

import "fmt"

type Type string

const (
	Keyword Type = "keyword"
	Text    Type = "text"
	Bool    Type = "bool"
	Slice   Type = "slice"
	Map     Type = "map"
)

func (t Type) Valid() bool {
	return t == Keyword || t == Text || t == Bool || t == Slice || t == Map
}

type Field struct {
	Name     string
	Type     Type
	Source   string
	Required bool

	Children []Field
}

type Schema struct {
	Fields []Field
}

func NewSchema(fields []Field) *Schema {
	return &Schema{
		Fields: fields,
	}
}

func (s *Schema) Validate() error {
	names := make(map[string]struct{})
	for _, f := range s.Fields {
		err := s.validateField(f, "")
		if err != nil {
			return err
		}

		if _, ok := names[f.Name]; ok {
			return fmt.Errorf("field %q is already defined", f.Name)
		}
		names[f.Name] = struct{}{}
	}

	return nil
}

func (s *Schema) validateField(f Field, path string) error {
	if f.Name == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	if path != "" {
		path += "."
	}
	path += f.Name

	if f.Source == "" {
		return fmt.Errorf("field %q source is empty", path)
	}

	if !f.Type.Valid() {
		return fmt.Errorf("invalid field %q type %q", path, f.Type)
	}
	if len(f.Children) != 0 {
		if f.Type != Slice && f.Type != Map {
			return fmt.Errorf("field %q type %q cannot have children types", path, f.Type)
		}

		for _, child := range f.Children {
			err := s.validateField(child, path)
			if err != nil {
				return err
			}
		}
	} else if f.Type == Slice || f.Type == Map {
		return fmt.Errorf("field %q type %q must have children defined", path, f.Type)
	}

	return nil
}
