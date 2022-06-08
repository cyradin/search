package index

import (
	"context"
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

func NewDocuments(ctx context.Context, i entity.Index, sourceStorage Storage[uint32, entity.DocSource], fieldPath string) (*Documents, error) {
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
		Type:     field.TypeAll,
	})

	for _, f := range fields {
		err := result.addField(ctx, f, fieldPath)
		if err != nil {
			return nil, fmt.Errorf("unable to add field: %w", err)
		}
	}

	return result, nil
}

func (d *Documents) addField(ctx context.Context, schemaField schema.Field, src string) error {
	d.fieldsMtx.Lock()
	defer d.fieldsMtx.Unlock()

	src = path.Join(src, schemaField.Name+".gob")
	var f field.Field

	switch schemaField.Type {
	case field.TypeAll:
		f = field.NewAll(ctx, src)
	case field.TypeBool:
		f = field.NewBool(ctx, src)
	case field.TypeKeyword:
		f = field.NewKeyword(ctx, src)
	case field.TypeText:
		f = field.NewText(ctx, src) // @todo pass analyzers from schema
	// @todo implement slice type
	// case field.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(ctx, src)
	// @todo implement map type
	// case field.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(ctx, src)
	case field.TypeUnsignedLong:
		f = field.NewUnsignedLong(ctx, src)
	case field.TypeLong:
		f = field.NewLong(ctx, src)
	case field.TypeInteger:
		f = field.NewInteger(ctx, src)
	case field.TypeShort:
		f = field.NewShort(ctx, src)
	case field.TypeByte:
		f = field.NewByte(ctx, src)
	case field.TypeDouble:
		f = field.NewDouble(ctx, src)
	case field.TypeFloat:
		f = field.NewFloat(ctx, src)
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
			err := f.AddValue(id, value)
			if err != nil {
				return id, fmt.Errorf("field value insert err: %w", err)
			}
			err = d.fields[field.AllField].AddValue(id, value)
			if err != nil {
				return id, fmt.Errorf("field value insert err: %w", err)
			}
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
