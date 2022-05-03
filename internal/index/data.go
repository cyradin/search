package index

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
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

	sourceStorage storage.Storage[entity.DocSource]
}

func NewData(ctx context.Context, index entity.Index, sourceStorage storage.Storage[entity.DocSource], fieldPath string) (*Data, error) {
	ids := NewIDs(0, nil)
	result := &Data{
		index:  index,
		fields: make(map[string]field.Field),

		idGet: ids.Get,
		idSet: ids.Set,

		sourceStorage: sourceStorage,
	}

	for _, f := range index.Schema.Fields {
		err := result.addField(ctx, f, fieldPath)
		if err != nil {
			return nil, fmt.Errorf("unable to add field: %w", err)
		}
	}

	return result, nil
}

func (d *Data) addField(ctx context.Context, schemaField schema.Field, src string) error {
	d.fieldsMtx.RLock()
	defer d.fieldsMtx.RUnlock()

	src = path.Join(src, schemaField.Name+".json")
	var (
		f   field.Field
		err error
	)

	switch schemaField.Type {
	case field.TypeBool:
		f, err = field.NewBool(ctx, src)
	case field.TypeKeyword:
		f, err = field.NewKeyword(ctx, src)
	case field.TypeText:
		f = field.NewText(ctx) // @todo pass analyzers from schema
	// @todo implement slice type
	// case field.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(ctx)
	// @todo implement map type
	// case field.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(ctx)
	case field.TypeUnsignedLong:
		f = field.NewUnsignedLong(ctx)
	case field.TypeLong:
		f = field.NewLong(ctx)
	case field.TypeInteger:
		f = field.NewInteger(ctx)
	case field.TypeShort:
		f = field.NewShort(ctx)
	case field.TypeByte:
		f = field.NewByte(ctx)
	case field.TypeDouble:
		f = field.NewDouble(ctx)
	case field.TypeFloat:
		f = field.NewFloat(ctx)
	default:
		return fmt.Errorf("invalid field type %q", schemaField.Type)
	}

	if err != nil {
		return err
	}

	d.fields[schemaField.Name] = f

	return nil
}

func (d *Data) Add(guid string, source entity.DocSource) (string, error) {
	if err := validateDoc(d.index.Schema, source); err != nil {
		return guid, err
	}

	guid, err := d.sourceStorage.Insert(guid, source)
	if err != nil {
		return guid, err
	}

	id := d.idSet(guid)

	for name, field := range d.fields {
		if v, ok := source[name]; ok {
			err := field.AddValue(id, v)
			if err != nil {
				return guid, err
			}
		}
	}

	return guid, nil
}

func (d *Data) Get(guid string) (entity.DocSource, error) {
	doc, err := d.sourceStorage.One(guid)
	if err != nil {
		return nil, err
	}

	return doc.Source, err
}
