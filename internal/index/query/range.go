package query

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/spf13/cast"
)

var _ Query = (*RangeQuery)(nil)

type RangeQuery struct {
	Field       string
	From        interface{} `json:"from"`
	To          interface{} `json:"to"`
	IncludeFrom bool        `json:"includeFrom"`
	IncludeTo   bool        `json:"includeTo"`
}

func (q *RangeQuery) Validate() error {
	return validation.ValidateStruct(q,
		validation.Field(&q.Field, validation.Required, validation.Length(1, 255)),
		validation.Field(&q.From, validation.Required.When(q.To == nil), validation.By(func(value interface{}) error {
			_, err := cast.ToFloat64E(value)
			return err
		})),
		validation.Field(&q.To, validation.Required.When(q.From == nil), validation.By(func(value interface{}) error {
			_, err := cast.ToFloat64E(value)
			return err
		})),
	)
}

func (q *RangeQuery) Exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)
	field, ok := fields[q.Field]
	if !ok {
		return newEmptyResult(), nil
	}

	return newResult(field.RangeQuery(ctx, q.From, q.To, q.IncludeTo, q.IncludeFrom)), nil
}
