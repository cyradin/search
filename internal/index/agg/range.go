package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Agg = (*RangeAgg)(nil)

type RangeResult struct {
	Buckets []RangeBucket
}

type RangeBucket struct {
	Key      interface{}            `json:"key"`
	DocCount int                    `json:"docCount"`
	From     interface{}            `json:"from"`
	To       interface{}            `json:"to"`
	Aggs     map[string]interface{} `json:"aggs,omitempty"`
}

type RangeAgg struct {
	Ranges []RangeAggRange `json:"ranges"`
	Field  string
	Aggs   Aggs `json:"aggs"`
}

func (a *RangeAgg) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Field, validation.Required),
		validation.Field(&a.Ranges, validation.Required, validation.Length(1, 0)),
	)
}

type RangeAggRange struct {
	Key  string      `json:"key"`
	From interface{} `json:"from"`
	To   interface{} `json:"to"`
}

func (r RangeAggRange) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.Key, validation.Required, validation.Length(1, 255)),
		validation.Field(&r.From, validation.Required.When(r.To == nil)),
		validation.Field(&r.To, validation.Required.When(r.From == nil)),
	)
}

func (a *RangeAgg) Exec(ctx context.Context, fields Fields, docs *roaring.Bitmap) (interface{}, error) {
	f, ok := fields[a.Field]
	if !ok {
		return RangeResult{}, nil
	}

	result := RangeResult{Buckets: make([]RangeBucket, len(a.Ranges))}
	for i, r := range a.Ranges {
		res := f.RangeQuery(ctx, r.From, r.To, true, true)
		rangeDocs := res.Docs()
		rangeDocs.And(docs)

		result.Buckets[i].From = res.From()
		result.Buckets[i].To = res.To()
		result.Buckets[i].Key = r.Key
		result.Buckets[i].DocCount = int(rangeDocs.GetCardinality())

		if len(a.Aggs) > 0 {
			result.Buckets[i].Aggs = make(map[string]interface{}, len(a.Aggs))
		}

		for key, subAgg := range a.Aggs {
			subAggResult, err := subAgg.Exec(ctx, fields, rangeDocs)
			if err != nil {
				return nil, err
			}
			result.Buckets[i].Aggs[key] = subAggResult
		}
	}

	return result, nil
}
