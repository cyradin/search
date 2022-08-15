package query

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/spf13/cast"
)

var _ Query = (*MatchQuery)(nil)

type MatchQuery struct {
	Field string      `json:"field"`
	Query interface{} `json:"query"`
}

func (q *MatchQuery) Validate() error {
	return validation.ValidateStruct(q,
		validation.Field(&q.Field, validation.Required, validation.Length(1, 255)),
		validation.Field(&q.Query, validation.NotNil, validation.By(func(value interface{}) error {
			_, err := cast.ToStringE(value)
			return err
		})),
	)
}

func (q *MatchQuery) Exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)
	field, ok := fields[q.Field]
	if !ok {
		return newEmptyResult(), nil
	}

	return newResult(field.MatchQuery(ctx, q.Query)), nil
}
