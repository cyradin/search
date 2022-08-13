package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Agg = (*TermsAgg)(nil)

const TermsAggDefaultSize = 10

type TermsResult struct {
	Buckets []TermsBucket
}

type TermsBucket struct {
	Key      interface{}            `json:"key"`
	DocCount int                    `json:"docCount"`
	Aggs     map[string]interface{} `json:"aggs,omitempty"`
}

type TermsAgg struct {
	Size  int    `json:"size"`
	Field string `json:"field"`
	Aggs  Aggs   `json:"aggs"`
}

func (a *TermsAgg) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Size, validation.Min(0)),
		validation.Field(&a.Field, validation.Required),
	)
}

func (a *TermsAgg) Exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error) {
	fields := fields(ctx)
	field, ok := fields[a.Field]
	if !ok {
		return TermsResult{}, nil
	}

	res := field.TermAgg(ctx, docs, a.Size)

	result := TermsResult{
		Buckets: make([]TermsBucket, len(res.Buckets)),
	}
	for i, b := range res.Buckets {
		result.Buckets[i] = TermsBucket{
			Key:      b.Key,
			DocCount: int(b.Docs.GetCardinality()),
		}

		if len(a.Aggs) > 0 {
			result.Buckets[i].Aggs = make(map[string]interface{}, len(a.Aggs))
			for key, subAgg := range a.Aggs {
				subAggResult, err := subAgg.Exec(ctx, b.Docs)
				if err != nil {
					return nil, err
				}
				result.Buckets[i].Aggs[key] = subAggResult
			}
		}
	}

	return result, nil
}
