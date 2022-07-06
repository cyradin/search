package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Query = (*termQuery)(nil)
var _ Query = (*termsQuery)(nil)

type termQuery struct {
	query Req
}

func newTermQuery(ctx context.Context, req Req) (*termQuery, error) {
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
				validation.Key("query", validation.Required),
			))
		}),
	)
	if err != nil {
		return nil, err
	}

	return &termQuery{
		query: req,
	}, nil
}

func (q *termQuery) exec(ctx context.Context) (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)
	fields := fields(ctx)
	field, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}
	v := val.(map[string]interface{})["query"]

	return field.Get(v), nil
}

type termsQuery struct {
	query Req
}

func newTermsQuery(ctx context.Context, req Req) (*termsQuery, error) {
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
				validation.Key("query", validation.Required, validation.WithContext(func(ctx context.Context, value interface{}) error {
					_, err := interfaceToSlice[interface{}](value)
					if err != nil {
						return errs.ArrayRequired(ctx, key)
					}
					return nil
				})),
			))
		}),
	)
	if err != nil {
		return nil, err
	}

	return &termsQuery{
		query: req,
	}, nil
}

func (q *termsQuery) exec(ctx context.Context) (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)
	fields := fields(ctx)
	field, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}
	v, _ := interfaceToSlice[interface{}](val.(map[string]interface{})["query"])

	return field.GetOr(v), nil
}
