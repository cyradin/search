package index

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cyradin/search/internal/document"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/google/uuid"
)

type Index struct {
	CreatedAt time.Time

	schema *schema.Schema

	fieldsMtx sync.RWMutex
	fields    map[string]field.Field

	idGet idGetter
	idSet idSetter

	sourceInsert sourceInserter

	guidGenerate func() string
}

func New(ctx context.Context, s *schema.Schema, storage document.Storage, maxID uint32) (*Index, error) {
	if err := schema.Validate(s); err != nil {
		return nil, err
	}

	ids := NewIDs(maxID, nil)

	result := &Index{
		CreatedAt: time.Now(),
		schema:    s,
		fields:    make(map[string]field.Field),

		idGet: ids.Get,
		idSet: ids.Set,

		guidGenerate: uuid.NewString,
		sourceInsert: storage.Insert,
	}

	for _, f := range s.Fields {
		err := result.addField(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("unable to add field: %w", err)
		}
	}

	return result, nil
}

func (i *Index) addField(ctx context.Context, f schema.Field) error {
	i.fieldsMtx.RLock()
	defer i.fieldsMtx.RUnlock()

	switch f.Type {
	case field.TypeBool:
		i.fields[f.Name] = field.NewBool(ctx)
	case field.TypeKeyword:
		i.fields[f.Name] = field.NewKeyword(ctx)
	case field.TypeText:
		i.fields[f.Name] = field.NewText(ctx) // @todo pass analyzers from schema
	// @todo implement slice type
	// case field.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice(ctx)
	// @todo implement map type
	// case field.TypeNap:
	// 	i.fields[f.Name] = field.NewMap(ctx)
	case field.TypeUnsignedLong:
		i.fields[f.Name] = field.NewUnsignedLong(ctx)
	case field.TypeLong:
		i.fields[f.Name] = field.NewLong(ctx)
	case field.TypeInteger:
		i.fields[f.Name] = field.NewInteger(ctx)
	case field.TypeShort:
		i.fields[f.Name] = field.NewShort(ctx)
	case field.TypeByte:
		i.fields[f.Name] = field.NewByte(ctx)
	case field.TypeDouble:
		i.fields[f.Name] = field.NewDouble(ctx)
	case field.TypeFloat:
		i.fields[f.Name] = field.NewFloat(ctx)
	default:
		return fmt.Errorf("invalid field type %q", f.Type)
	}

	return nil
}
