package index

import (
	"context"
	"fmt"
	"time"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
)

type Index struct {
	CreatedAt time.Time
	schema    *schema.Schema
	fields    map[string]field.Field
}

func New(ctx context.Context, s *schema.Schema) (*Index, error) {
	if err := schema.Validate(s); err != nil {
		return nil, err
	}

	result := &Index{
		CreatedAt: time.Now(),
		schema:    s,
		fields:    make(map[string]field.Field),
	}

	for _, f := range s.Fields {
		err := result.addField(ctx, f)
		if err != nil {
			return nil, fmt.Errorf("cannot add field: %w", err)
		}
	}

	return result, nil
}

func (i *Index) addField(ctx context.Context, f schema.Field) error {
	ff, err := field.NewField(ctx, f.Type)
	if err != nil {
		return err
	}
	i.fields[f.Name] = ff

	return nil
}
