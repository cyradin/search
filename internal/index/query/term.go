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
	err := validation.Validate(req,
		validation.Required.ErrorObject(errs.Required(ctx)),
		validation.Length(1, 1).ErrorObject(errs.SingleKeyRequired(ctx)),
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

	return field.Get(val), nil
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
			_, err := interfaceToSlice[interface{}](val)
			if err != nil {
				return errs.ArrayRequired(ctx, key)
			}
			return nil
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
	values, _ := interfaceToSlice[interface{}](val)
	fields := fields(ctx)
	field, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.GetOr(values), nil
}
