package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Agg = (*MaxAgg)(nil)

type MaxResult struct {
	Value interface{}            `json:"value"`
	Aggs  map[string]interface{} `json:"aggs,omitempty"`
}

type MaxAgg struct {
	Field string `json:"field"`
	Aggs  Aggs   `json:"aggs"`
}

func (a *MaxAgg) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Field, validation.Required),
	)
}

func (a *MaxAgg) Exec(ctx context.Context, fields Fields, docs *roaring.Bitmap) (interface{}, error) {
	field, ok := fields[a.Field]
	if !ok {
		return TermsResult{}, nil
	}

	val, resDocs := field.MaxValue()
	resDocs.And(docs)

	if resDocs.IsEmpty() {
		return MaxResult{Value: nil}, nil
	}

	result := MaxResult{Value: val}
	if len(a.Aggs) > 0 {
		result.Aggs = make(map[string]interface{}, len(a.Aggs))
		for key, subAgg := range a.Aggs {
			subAggResult, err := subAgg.Exec(ctx, fields, resDocs)
			if err != nil {
				return nil, err
			}
			result.Aggs[key] = subAggResult
		}
	}

	return result, nil
}
