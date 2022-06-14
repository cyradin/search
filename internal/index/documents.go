package index

import (
	"fmt"
	"path"
	"sync"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/query"
	"github.com/cyradin/search/internal/index/schema"
)

type (
	idGetter func(uid string) uint32
	idSetter func(uid string) uint32

	sourceStorage interface {
		Insert(guid string, doc entity.DocSource) error
		All() (<-chan entity.DocSource, <-chan error)
	}
)

type Documents struct {
	index entity.Index

	fieldsMtx sync.RWMutex
	fields    map[string]field.Field

	sourceStorage Storage[uint32, entity.DocSource]
}

func NewDocuments(i entity.Index, sourceStorage Storage[uint32, entity.DocSource], fieldPath string) (*Documents, error) {
	result := &Documents{
		index:  i,
		fields: make(map[string]field.Field),

		sourceStorage: sourceStorage,
	}

	// add "allField" which contains all documents
	fields := make([]schema.Field, len(i.Schema.Fields))
	copy(fields, i.Schema.Fields)
	fields = append(fields, schema.Field{
		Name:     field.AllField,
		Required: false,
		Type:     schema.TypeAll,
	})

	for _, f := range fields {
		err := result.addField(f, fieldPath)
		if err != nil {
			return nil, fmt.Errorf("unable to add field: %w", err)
		}
	}

	return result, nil
}

func (d *Documents) addField(schemaField schema.Field, src string) error {
	d.fieldsMtx.Lock()
	defer d.fieldsMtx.Unlock()

	src = path.Join(src, schemaField.Name+".gob")
	var f field.Field

	switch schemaField.Type {
	case schema.TypeAll:
		f = field.NewAll(src)
	case schema.TypeBool:
		f = field.NewBool(src)
	case schema.TypeKeyword:
		f = field.NewKeyword(src)
	case schema.TypeText:
		f = field.NewText(src) // @todo pass analyzers from schema
	// @todo implement slice type
	// case schema.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(src)
	// @todo implement map type
	// case schema.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(src)
	case schema.TypeUnsignedLong:
		f = field.NewUnsignedLong(src)
	case schema.TypeLong:
		f = field.NewLong(src)
	case schema.TypeInteger:
		f = field.NewInteger(src)
	case schema.TypeShort:
		f = field.NewShort(src)
	case schema.TypeByte:
		f = field.NewByte(src)
	case schema.TypeDouble:
		f = field.NewDouble(src)
	case schema.TypeFloat:
		f = field.NewFloat(src)
	default:
		return fmt.Errorf("invalid field type %q", schemaField.Type)
	}

	err := f.Init()
	if err != nil {
		return fmt.Errorf("field init err: %w", err)
	}

	d.fields[schemaField.Name] = f

	return nil
}

func (d *Documents) Add(id uint32, source entity.DocSource) (uint32, error) {
	if err := schema.ValidateDoc(d.index.Schema, source); err != nil {
		return 0, fmt.Errorf("source validation err: %w", err)
	}

	id, err := d.sourceStorage.Insert(id, source)
	if err != nil {
		return id, fmt.Errorf("source insert err: %w", err)
	}

	for key, value := range source {
		if f, ok := d.fields[key]; ok {
			f.AddValue(id, value)
			d.fields[field.AllField].AddValue(id, value)
		}
	}

	return id, nil
}

func (d *Documents) Get(id uint32) (entity.DocSource, error) {
	doc, err := d.sourceStorage.One(id)
	if err != nil {
		return nil, fmt.Errorf("source get err: %w", err)
	}

	return doc.Source, err
}

func (d *Documents) Search(q entity.Search) (entity.SearchResult, error) {
	hits, err := query.Exec(q.Query, d.fields)
	if err != nil {
		return entity.SearchResult{}, err
	}

	fmt.Println(hits) // @todo make search result

	return entity.SearchResult{}, nil
}
