package query

import (
	"context"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*boolQuery)(nil)

type boolQuery struct {
	query Query

	must   []internalQuery
	should []internalQuery
	filter []internalQuery
}

func newBoolQuery(ctx context.Context, query Query) (*boolQuery, error) {
	err := validation.ValidateWithContext(ctx, query, validation.Map(
		validation.Key(string(queryBoolMust), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolMust))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolShould), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolShould))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolFilter), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolFilter))
			}
			return nil
		})).Optional(),
	))
	if err != nil {
		return nil, err
	}

	result := &boolQuery{
		query: query,
	}

	for key, value := range query {
		values, _ := interfaceToSlice[map[string]interface{}](value)

		children := make([]internalQuery, len(values))
		for i, v := range values {
			ctx := errs.WithPath(ctx, errs.Path(ctx), key)
			child, err := build(ctx, v)
			if err != nil {
				return nil, err
			}
			children[i] = child
		}

		switch queryType(key) {
		case queryBoolShould:
			result.should = children
		case queryBoolMust:
			result.must = children
		case queryBoolFilter:
			result.filter = children
		}
	}

	return result, nil
}

func (q *boolQuery) exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)

	if len(q.should) == 0 && len(q.must) == 0 && len(q.filter) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			return newResult(ff.Term(ctx, true)), nil
		}

		return newEmptyResult(), nil
	}

	shouldResult, err := q.execType(ctx, q.should, func(src *queryResult, dst *queryResult) { src.Or(dst) })
	if err != nil {
		return newEmptyResult(), err
	}

	filterResult, err := q.execType(ctx, q.filter, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return newEmptyResult(), err
	}

	mustResult, err := q.execType(ctx, q.must, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return newEmptyResult(), err
	}

	var result *queryResult
	if shouldResult != nil {
		result = shouldResult
	}
	if mustResult != nil {
		if result == nil {
			result = mustResult
		} else {
			result.And(mustResult)
		}
	}
	if filterResult != nil {
		if result == nil {
			result = filterResult
		} else {
			result.And(filterResult)
		}
	}

	return result, nil
}

func (q *boolQuery) execType(
	ctx context.Context,
	queries []internalQuery,
	apply func(src *queryResult, dst *queryResult),
) (*queryResult, error) {
	var result *queryResult

	for _, query := range queries {
		r, err := query.exec(ctx)
		if err != nil {
			return newEmptyResult(), err
		}

		if result == nil {
			result = r
			continue
		}

		apply(result, r)
	}

	return result, nil
}
