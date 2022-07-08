package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*termQuery)(nil)
var _ internalQuery = (*termsQuery)(nil)

type termQuery struct {
	query Query
}

func newTermQuery(ctx context.Context, query Query) (*termQuery, error) {
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

	return &termQuery{
		query: query,
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
	query Query
}

func newTermsQuery(ctx context.Context, query Query) (*termsQuery, error) {
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
				validation.Key(
					"query",
					validation.NotNil.ErrorObject(errs.Required(ctx)),
					validation.WithContext(func(ctx context.Context, value interface{}) error {
						_, err := interfaceToSlice[interface{}](value)
						if err != nil {
							return errs.ArrayRequired(ctx, key)
						}
						return nil
					}),
				),
			))
		}),
	)
	if err != nil {
		return nil, err
	}

	return &termsQuery{
		query: query,
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
