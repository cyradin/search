package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/spf13/cast"
)

var _ internalAgg = (*termsAgg)(nil)

const TermsAggDefaultSize = 10

type Terms struct {
	Buckets []TermsBucket
}

type TermsBucket struct {
	Key      interface{}            `json:"key"`
	DocCount int                    `json:"docCount"`
	SubAggs  map[string]interface{} `json:"subAggs,omitempty"`
}

type termsAgg struct {
	size    int
	field   string
	subAggs map[string]internalAgg
}

func newTermsAgg(ctx context.Context, req Aggs, subAggsReq Aggs) (*termsAgg, error) {
	err := validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
		validation.Map(
			validation.Key("size", validation.WithContext(valid.JsonNumberIntMin(0))),
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

	sz := cast.ToInt(req["size"])
	if sz == 0 {
		sz = TermsAggDefaultSize
	}

	return &termsAgg{
		size:    sz,
		field:   cast.ToString(req["field"]),
		subAggs: subAggs,
	}, nil
}

func (a *termsAgg) exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error) {
	fields := fields(ctx)
	field, ok := fields[a.field]
	if !ok {
		return nil, nil
	}

	res := field.TermAgg(ctx, docs, a.size)

	result := Terms{
		Buckets: make([]TermsBucket, len(res.Buckets)),
	}
	for i, b := range res.Buckets {
		result.Buckets[i] = TermsBucket{
			Key:      b.Key,
			DocCount: int(b.Docs.GetCardinality()),
		}

		if len(a.subAggs) > 0 {
			result.Buckets[i].SubAggs = make(map[string]interface{}, len(a.subAggs))
			for key, subAgg := range a.subAggs {
				subAggResult, err := subAgg.exec(ctx, b.Docs)
				if err != nil {
					return nil, err
				}
				result.Buckets[i].SubAggs[key] = subAggResult
			}
		}
	}

	return result, nil
}
