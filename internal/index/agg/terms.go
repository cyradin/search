package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalAgg = (*termsAgg)(nil)

type termsAgg struct {
	agg     Aggs
	subAggs map[string]internalAgg
}

func newTermsAgg(ctx context.Context, req Aggs, subAggsReq Aggs) (*termsAgg, error) {
	err := validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
		validation.Map(
			validation.Key("size", validation.WithContext(valid.JsonNumberIntMin(1))),
			validation.Key("field", validation.Required, validation.WithContext(valid.String())),
		),
	)
	if err != nil {
		return nil, err
	}

	subAggs, err := build(ctx, subAggsReq)
	if err != nil {
		return nil, err
	}

	return &termsAgg{
		agg:     req,
		subAggs: subAggs,
	}, nil
}

func (a *termsAgg) exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error) {
	return nil, nil // @todo
}
