package query

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/spf13/cast"
)

var _ Query = (*TermQuery)(nil)
var _ Query = (*TermsQuery)(nil)

type TermQuery struct {
	Field string      `json:"field"`
	Query interface{} `json:"query"`
}

func (q *TermQuery) Validate() error {
	return validation.ValidateStruct(q,
		validation.Field(&q.Field, validation.Required, validation.Length(1, 255)),
		validation.Field(&q.Query, validation.NotNil, validation.By(func(value interface{}) error {
			_, err := cast.ToStringE(value)
			return err
		})),
	)
}

func (q *TermQuery) Exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)
	field, ok := fields[q.Field]
	if !ok {
		return newEmptyResult(), nil
	}

	return newResult(field.TermQuery(ctx, q.Query)), nil
}

type TermsQuery struct {
	Field string        `json:"field"`
	Query []interface{} `json:"query"`
}

func (q *TermsQuery) Validate() error {
	return validation.ValidateStruct(q,
		validation.Field(&q.Field, validation.Required, validation.Length(1, 255)),
		validation.Field(&q.Query, validation.Required, validation.Each(validation.By(func(value interface{}) error {
			_, err := cast.ToStringE(value)
			return err
		}))),
	)
}

func (q *TermsQuery) Exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)
	field, ok := fields[q.Field]
	if !ok {
		return newEmptyResult(), nil
	}

	var result *queryResult
	for _, v := range q.Query {
		r := newResult(field.TermQuery(ctx, v))
		if result == nil {
			result = r
			continue
		}
		result.Or(result)
	}

	return result, nil
}
