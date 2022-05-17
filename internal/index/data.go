package index

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
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

type Data struct {
	index entity.Index

	fieldsMtx sync.RWMutex
	fields    map[string]field.Field

	idGet idGetter
	idSet idSetter

	sourceStorage Storage[entity.DocSource]
}

func NewData(ctx context.Context, i entity.Index, sourceStorage Storage[entity.DocSource], fieldPath string) (*Data, error) {
	ids := NewIDs(0, nil)
	result := &Data{
		index:  i,
		fields: make(map[string]field.Field),

		idGet: ids.Get,
		idSet: ids.Set,

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

func (d *Data) addField(ctx context.Context, schemaField schema.Field, src string) error {
	d.fieldsMtx.Lock()
	defer d.fieldsMtx.Unlock()

	src = path.Join(src, schemaField.Name+".gob")
	var (
		f   field.Field
		err error
	)

	switch schemaField.Type {
	case field.TypeAll:
		f, err = field.NewAll(ctx, src)
	case field.TypeBool:
		f, err = field.NewBool(ctx, src)
	case field.TypeKeyword:
		f, err = field.NewKeyword(ctx, src)
	case field.TypeText:
		f, err = field.NewText(ctx, src) // @todo pass analyzers from schema
	// @todo implement slice type
	// case field.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(ctx, src)
	// @todo implement map type
	// case field.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(ctx, src)
	case field.TypeUnsignedLong:
		f, err = field.NewUnsignedLong(ctx, src)
	case field.TypeLong:
		f, err = field.NewLong(ctx, src)
	case field.TypeInteger:
		f, err = field.NewInteger(ctx, src)
	case field.TypeShort:
		f, err = field.NewShort(ctx, src)
	case field.TypeByte:
		f, err = field.NewByte(ctx, src)
	case field.TypeDouble:
		f, err = field.NewDouble(ctx, src)
	case field.TypeFloat:
		f, err = field.NewFloat(ctx, src)
	default:
		return fmt.Errorf("invalid field type %q", schemaField.Type)
	}

	if err != nil {
		return fmt.Errorf("field init err: %w", err)
	}

	d.fields[schemaField.Name] = f

	return nil
}

func (d *Data) Add(guid string, source entity.DocSource) (string, error) {
	if err := validateDoc(d.index.Schema, source); err != nil {
		return guid, fmt.Errorf("source validation err: %w", err)
	}

	guid, err := d.sourceStorage.Insert(guid, source)
	if err != nil {
		return guid, fmt.Errorf("source insert err: %w", err)
	}

	id := d.idSet(guid)

	for key, value := range source {
		if f, ok := d.fields[key]; ok {
			err := f.AddValue(id, value)
			if err != nil {
				return guid, fmt.Errorf("field value insert err: %w", err)
			}
			err = d.fields[field.AllField].AddValue(id, value)
			if err != nil {
				return guid, fmt.Errorf("field value insert err: %w", err)
			}
		}
	}

	return guid, nil
}

func (d *Data) Get(guid string) (entity.DocSource, error) {
	doc, err := d.sourceStorage.One(guid)
	if err != nil {
		return nil, fmt.Errorf("source get err: %w", err)
	}

	return doc.Source, err
}
