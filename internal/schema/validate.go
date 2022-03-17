package schema

import "fmt"

func Validate(s *Schema) error {
	names := make(map[string]struct{})
	for _, f := range s.Fields {
		err := validateField(f, "")
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

func validateField(f Field, path string) error {
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
			err := validateField(child, path)
			if err != nil {
				return err
			}
		}
	} else if f.Type == Slice || f.Type == Map {
		return fmt.Errorf("field %q type %q must have children defined", path, f.Type)
	}

	return nil
}
