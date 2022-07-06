package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Query = (*matchQuery)(nil)

type matchQuery struct {
	query Req
}

func newMatchQuery(ctx context.Context, req Req) (*matchQuery, error) {
	err := validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(errs.Required(ctx)),
		validation.Length(1, 1).ErrorObject(errs.SingleKeyRequired(ctx)),
		validation.WithContext(func(ctx context.Context, value interface{}) error {
			key, val := firstVal(value.(Req))
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
		query: req,
	}, nil
}

func (q *matchQuery) exec(ctx context.Context) (*roaring.Bitmap, error) {
	return nil, nil
}
