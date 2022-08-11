package query

import (
	"context"

	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*matchQuery)(nil)

type matchQuery struct {
	query Query
}

func newMatchQuery(ctx context.Context, query Query) (*matchQuery, error) {
	err := validation.ValidateWithContext(ctx, query,
		validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
		validation.Length(1, 1).ErrorObject(valid.NewErrSingleKeyRequired(ctx)),
		validation.WithContext(func(ctx context.Context, value interface{}) error {
			key, val := firstVal(value.(Query))
			ctx = valid.WithPath(ctx, valid.Path(ctx), key)

			v, ok := val.(map[string]interface{})
			if !ok {
				return valid.NewErrObjectRequired(ctx, key)
			}
			return validation.ValidateWithContext(ctx, v, validation.Map(
				validation.Key("query", validation.NotNil.ErrorObject(valid.NewErrRequired(ctx))),
			))
		}),
	)
	if err != nil {
		return nil, err
	}

	return &matchQuery{
		query: query,
	}, nil
}

func (q *matchQuery) exec(ctx context.Context) (*queryResult, error) {
	key, val := firstVal(q.query)
	fields := fields(ctx)
	f, ok := fields[key]
	if !ok {
		return newEmptyResult(), nil
	}

	v := val.(map[string]interface{})["query"]

	return newResult(f.MatchQuery(ctx, v)), nil
}
