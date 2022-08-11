package query

import (
	"context"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*boolQuery)(nil)

type boolQuery struct {
	query    Query
	parallel bool

	must   []internalQuery
	should []internalQuery
	filter []internalQuery
}

func newBoolQuery(ctx context.Context, query Query) (*boolQuery, error) {
	err := validation.ValidateWithContext(ctx, query, validation.Map(
		validation.Key(string(queryBoolMust), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return valid.NewErrArrayRequired(ctx, string(queryBoolMust))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolShould), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return valid.NewErrArrayRequired(ctx, string(queryBoolShould))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolFilter), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return valid.NewErrArrayRequired(ctx, string(queryBoolFilter))
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
			ctx := valid.WithPath(ctx, valid.Path(ctx), key)
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
			return newResult(ff.TermQuery(ctx, true)), nil
		}

		return newEmptyResult(), nil
	}

	var (
		shouldResult, filterResult, mustResult *queryResult
		err                                    error
	)

	if q.parallel {
		shouldResult, filterResult, mustResult, err = q.runParallel(ctx)
	} else {
		shouldResult, filterResult, mustResult, err = q.runSync(ctx)
	}
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

func (q *boolQuery) runParallel(ctx context.Context) (*queryResult, *queryResult, *queryResult, error) {
	errors := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chShould := q.runAsyncQueries(ctx, q.should, errors)
	chMust := q.runAsyncQueries(ctx, q.must, errors)
	chFilter := q.runAsyncQueries(ctx, q.filter, errors)

	var shouldResult *queryResult
	var filterResult *queryResult
	var mustResult *queryResult

	for i := 0; i < len(q.should)+len(q.must)+len(q.filter); i++ {
		select {
		case r := <-chShould:
			if shouldResult == nil {
				shouldResult = r
			} else {
				shouldResult.Or(r)
			}
		case r := <-chMust:
			if mustResult == nil {
				mustResult = r
			} else {
				mustResult.And(r)
			}
		case r := <-chFilter:
			if filterResult == nil {
				filterResult = r
			} else {
				filterResult.And(r)
			}
		case err := <-errors:
			return nil, nil, nil, err
		}
	}

	return shouldResult, filterResult, mustResult, nil

}

func (q *boolQuery) runAsyncQueries(ctx context.Context, queries []internalQuery, errors chan<- error) <-chan *queryResult {
	ch := make(chan *queryResult)

	go func() {
		for _, query := range queries {
			go func(ctx context.Context, query internalQuery) {
				res, err := query.exec(ctx)
				if err != nil {
					errors <- err
				}
				ch <- res
			}(ctx, query)
		}
	}()

	return ch
}

func (q *boolQuery) runSync(ctx context.Context) (*queryResult, *queryResult, *queryResult, error) {
	shouldResult, err := q.runSyncQueries(ctx, q.should, func(src *queryResult, dst *queryResult) { src.Or(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	filterResult, err := q.runSyncQueries(ctx, q.filter, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	mustResult, err := q.runSyncQueries(ctx, q.must, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	return shouldResult, filterResult, mustResult, nil
}

func (q *boolQuery) runSyncQueries(
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
