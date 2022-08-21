package index

import (
	"fmt"
	"time"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
)

type IndexData struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Schema    schema.Schema `json:"schema"`
}

var ErrDocNotFound = fmt.Errorf("doc not found")

type DocSource map[string]interface{}

type Index struct {
	data   IndexData
	ids    *IDs
	fields map[string]field.Field
}

func NewIndex(i IndexData) (*Index, error) {
	result := &Index{
		data:   i,
		ids:    NewIDs(),
		fields: make(map[string]field.Field),
	}

	// add "allField" which contains all documents
	fieldsCopy := make(map[string]schema.Field)
	for name, field := range i.Schema.Fields {
		fieldsCopy[name] = field
	}
	fieldsCopy[field.AllField] = schema.NewField(schema.TypeAll, false, "")

	for name, f := range fieldsCopy {
		opts := field.Opts{}

		if f.Analyzer != "" {
			a, err := i.Schema.Analyzers[f.Analyzer].Build()
			if err != nil {
				return nil, errs.Errorf("analyzer build err: %w", err)
			}
			opts.Analyzer = a
		}

		if f.Type == schema.TypeText {
			opts.Scoring = field.NewScoring()
		}

		field, err := field.New(f.Type, opts)
		if err != nil {
			return nil, errs.Errorf("field build err: %w", err)
		}
		result.fields[name] = field
	}

	return result, nil
}

func (i *Index) Data() IndexData {
	return i.data
}

func (i *Index) Add(guid string, source DocSource) (string, error) {
	if guid == "" {
		guid = newGUID()
	}

	if err := schema.ValidateDoc(i.data.Schema, source); err != nil {
		return guid, errs.Errorf("doc validation err: %w", err)
	}

	id, err := i.ids.NextID(guid)
	if err != nil {
		return guid, errs.Errorf("doc get next id err: %w", err)
	}

	i.fields[field.AllField].Add(id, true)
	for key, value := range source {
		if f, ok := i.fields[key]; ok {
			f.Add(id, value)
		}
	}

	return guid, nil
}

func (i *Index) Get(guid string) (DocSource, error) {
	id := i.ids.ID(guid)
	if id == 0 {
		return nil, ErrDocNotFound
	}

	if res := i.fields[field.AllField].Data(id); !res[0].(bool) {
		// @todo warning?
		return nil, ErrDocNotFound
	}

	result := make(map[string]interface{})
	for k, f := range i.fields {
		if k == field.AllField {
			continue
		}
		result[k] = f.Data(id)
	}
	return result, nil
}

func (i *Index) Delete(guid string) error {
	if guid == "" {
		return errs.Errorf("doc guid is required")
	}

	id := i.ids.ID(guid)
	if id == 0 {
		return nil
	}

	for _, field := range i.fields {
		field.DeleteDoc(id)
	}
	i.ids.Delete(guid)

	return nil
}
