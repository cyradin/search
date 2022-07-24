package query

import (
	"context"

	"github.com/cyradin/search/internal/errs"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*rangeQuery)(nil)

type rangeQuery struct {
	query Query
}

func newRangeQuery(ctx context.Context, query Query) (*rangeQuery, error) {
	err := validation.ValidateWithContext(ctx, query,
		validation.Required.ErrorObject(errs.Required(ctx)),
		validation.Length(1, 1).ErrorObject(errs.SingleKeyRequired(ctx)),
		validation.WithContext(func(ctx context.Context, value interface{}) error {
			key, val := firstVal(value.(Query))
			ctx = errs.WithPath(ctx, errs.Path(ctx), key)

			v, ok := val.(map[string]interface{})
			if !ok {
				return errs.ObjectRequired(ctx, key)
			}
			return validation.ValidateWithContext(ctx, v,
				validation.Required.ErrorObject(errs.Required(ctx)),
				validation.Map(
					validation.Key("from", validation.NotNil.ErrorObject(errs.Required(ctx))).Optional(),
					validation.Key("includeLower", validation.NotNil.ErrorObject(errs.Required(ctx))).Optional(),
					validation.Key("to", validation.NotNil.ErrorObject(errs.Required(ctx))).Optional(),
					validation.Key("includeUpper", validation.NotNil.ErrorObject(errs.Required(ctx))).Optional(),
				),
			)
		}),
	)

	if err != nil {
		return nil, err
	}

	return &rangeQuery{
		query: query,
	}, nil
}

func (q *rangeQuery) exec(ctx context.Context) (*queryResult, error) {
	return nil, nil // @todo
}
