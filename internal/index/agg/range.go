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

func (a *RangeAgg) Exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error) {
	// @todo
	return nil, nil
}
