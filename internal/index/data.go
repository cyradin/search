package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
)

type (
	DocSource map[string]interface{}

	idGetter func(uid string) uint32
	idSetter func(uid string) uint32

	sourceStorage interface {
		Insert(guid string, doc DocSource) error
		All() (<-chan DocSource, <-chan error)
	}
)

type Data struct {
	index *Index

	fieldsMtx sync.RWMutex
	fields    map[string]field.Field

	idGet idGetter
	idSet idSetter

	sourceStorage storage.Storage[DocSource]
}

func NewData(ctx context.Context, index *Index, sourceStorage storage.Storage[DocSource]) (*Data, error) {
	ids := NewIDs(0, nil)
	result := &Data{
		index:  index,
		fields: make(map[string]field.Field),

		idGet: ids.Get,
		idSet: ids.Set,

		sourceStorage: sourceStorage,
	}

	for _, f := range index.Schema.Fields {
		err := result.addField(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("unable to add field: %w", err)
		}
	}

	return result, nil
}

func (d *Data) addField(ctx context.Context, f schema.Field) error {
	d.fieldsMtx.RLock()
	defer d.fieldsMtx.RUnlock()

	switch f.Type {
	case field.TypeBool:
		d.fields[f.Name] = field.NewBool(ctx)
	case field.TypeKeyword:
		d.fields[f.Name] = field.NewKeyword(ctx)
	case field.TypeText:
		d.fields[f.Name] = field.NewText(ctx) // @todo pass analyzers from schema
	// @todo implement slice type
	// case field.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(ctx)
	// @todo implement map type
	// case field.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(ctx)
	case field.TypeUnsignedLong:
		d.fields[f.Name] = field.NewUnsignedLong(ctx)
	case field.TypeLong:
		d.fields[f.Name] = field.NewLong(ctx)
	case field.TypeInteger:
		d.fields[f.Name] = field.NewInteger(ctx)
	case field.TypeShort:
		d.fields[f.Name] = field.NewShort(ctx)
	case field.TypeByte:
		d.fields[f.Name] = field.NewByte(ctx)
	case field.TypeDouble:
		d.fields[f.Name] = field.NewDouble(ctx)
	case field.TypeFloat:
		d.fields[f.Name] = field.NewFloat(ctx)
	default:
		return fmt.Errorf("invalid field type %q", f.Type)
	}

	return nil
}

func (d *Data) Add(guid string, source map[string]interface{}) (string, error) {
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
