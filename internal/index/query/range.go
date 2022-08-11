package query

import (
	"context"

	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/spf13/cast"
)

var _ internalQuery = (*rangeQuery)(nil)

type rangeQuery struct {
	query Query
}

func newRangeQuery(ctx context.Context, query Query) (*rangeQuery, error) {
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
			return validation.ValidateWithContext(ctx, v,
				validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
				validation.Map(
					validation.Key("from", validation.NotNil.ErrorObject(valid.NewErrRequired(ctx))).Optional(),
					validation.Key("includeLower", validation.NotNil.ErrorObject(valid.NewErrRequired(ctx))).Optional(),
					validation.Key("to", validation.NotNil.ErrorObject(valid.NewErrRequired(ctx))).Optional(),
					validation.Key("includeUpper", validation.NotNil.ErrorObject(valid.NewErrRequired(ctx))).Optional(),
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
	key, val := firstVal(q.query)
	fields := fields(ctx)
	field, ok := fields[key]
	if !ok {
		return newEmptyResult(), nil
	}

	vv := val.(map[string]interface{})

	from := vv["from"]
	to := vv["to"]
	includeLower := cast.ToBool(vv["includeLower"])
	includeUpper := cast.ToBool(vv["includeUpper"])

	return newResult(field.RangeQuery(ctx, from, to, includeLower, includeUpper)), nil
}
