package agg

import (
	"context"

	"github.com/cyradin/search/internal/index/field"
)

type Fields map[string]field.Field

func withFields(ctx context.Context, fields Fields) context.Context {
	return context.WithValue(ctx, "queryFields", fields)
}

func fields(ctx context.Context) Fields {
	v, ok := ctx.Value("queryFields").(Fields)
	if ok {
		return v
	}
	return nil
}
