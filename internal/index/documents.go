package index

import (
	"fmt"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
)

var ErrDocNotFound = fmt.Errorf("doc not found")

type DocSource map[string]interface{}

type Documents struct {
	fields *field.Storage
	ids    *IDs
}

func NewDocuments(dataPath string) *Documents {
	result := &Documents{
		fields: field.NewStorage(dataPath),
		ids:    NewIDs(),
	}

	return result
}

func (d *Documents) AddIndex(index Index) error {
	_, err := d.fields.AddIndex(index.Name, index.Schema)
	if err != nil {
		return err
	}

	return nil
}

func (d *Documents) DeleteIndex(name string) error {
	d.fields.DeleteIndex(name)

	return nil
}

func (d *Documents) Add(index Index, guid string, source DocSource) (string, error) {
	if guid == "" {
		guid = newGUID()
	}

	if err := schema.ValidateDoc(index.Schema, source); err != nil {
		return guid, errs.Errorf("doc validation err: %w", err)
	}

	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return guid, err
	}

	id, err := d.ids.NextID(guid)
	if err != nil {
		return guid, errs.Errorf("doc get next id err: %w", err)
	}

	fieldIndex.Add(id, source)

	return guid, nil
}

func (d *Documents) Get(index Index, guid string) (DocSource, error) {
	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return nil, err
	}

	id := d.ids.ID(guid)
	if id == 0 {
		return nil, ErrDocNotFound
	}

	doc, err := fieldIndex.Get(id)
	if err != nil {
		if err == field.ErrDocNotFound {
			return nil, ErrDocNotFound
		}
		return nil, errs.Errorf("document get err: %w", err)
	}

	return doc, nil
}

func (d *Documents) Delete(index Index, guid string) error {
	if guid == "" {
		return errs.Errorf("doc guid is required")
	}

	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return err
	}

	id := d.ids.ID(guid)
	if id == 0 {
		return nil
	}
	fieldIndex.Delete(id)
	d.ids.Delete(guid)

	return nil
}
