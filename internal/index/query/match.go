package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*matchQuery)(nil)

type matchQuery struct {
	query Query
}

func newMatchQuery(ctx context.Context, query Query) (*matchQuery, error) {
	err := validation.ValidateWithContext(ctx, query,
		validation.Required.ErrorObject(errs.Required(ctx)),
		validation.Length(1, 1).ErrorObject(errs.SingleKeyRequired(ctx)),
		validation.WithContext(func(ctx context.Context, value interface{}) error {
			key, val := firstVal(value.(Query))
			ctx = errs.WithPath(ctx, errs.Path(ctx), key)

			v, ok := val.(map[string]interface{})
			if !ok {
				return errs.ObjectRequired(ctx, key)
			}
			return validation.ValidateWithContext(ctx, v, validation.Map(
				validation.Key("query", validation.NotNil.ErrorObject(errs.Required(ctx))),
			))
		}),
	)
	if err != nil {
		return nil, err
	}

	return &matchQuery{
		query: query,
	}, nil
}

func (q *matchQuery) exec(ctx context.Context) (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)
	fields := fields(ctx)
	f, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}

	v := val.(map[string]interface{})["query"]
	if fts, ok := f.(field.FTS); ok {
		return fts.GetOrAnalyzed(v), nil
	}

	return f.Get(v), nil
}
