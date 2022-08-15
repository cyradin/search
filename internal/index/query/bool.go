package query

import (
	"bytes"
	"context"

	"github.com/cyradin/search/internal/index/field"
	jsoniter "github.com/json-iterator/go"
)

var _ Query = (*BoolQuery)(nil)

type BoolQuery struct {
	Must     []Query `json:"must"`
	Should   []Query `json:"should"`
	Filter   []Query `json:"filter"`
	Parallel bool    `json:"parallel"`
}

func (q *BoolQuery) UnmarshalJSON(data []byte) error {
	d := struct {
		Must     []jsoniter.RawMessage `json:"must"`
		Should   []jsoniter.RawMessage `json:"should"`
		Filter   []jsoniter.RawMessage `json:"filter"`
		Parallel bool                  `json:"parallel"`
	}{}

	dec := jsoniter.NewDecoder(bytes.NewBuffer(data))
	dec.UseNumber()
	err := dec.Decode(&d)
	if err != nil {
		return err
	}
	q.Parallel = d.Parallel

	q.Must = make([]Query, len(d.Must))
	for i, r := range d.Must {
		v, err := build(QueryRequest(r))
		if err != nil {
			return err
		}
		q.Must[i] = v
	}

	q.Should = make([]Query, len(d.Should))
	for i, r := range d.Should {
		v, err := build(QueryRequest(r))
		if err != nil {
			return err
		}
		q.Should[i] = v
	}

	q.Filter = make([]Query, len(d.Filter))
	for i, r := range d.Filter {
		v, err := build(QueryRequest(r))
		if err != nil {
			return err
		}
		q.Filter[i] = v
	}

	return nil
}

func (q *BoolQuery) Validate() error {
	return nil
}

func (q *BoolQuery) Exec(ctx context.Context) (*queryResult, error) {
	fields := fields(ctx)

	if len(q.Should) == 0 && len(q.Must) == 0 && len(q.Filter) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			return newResult(ff.TermQuery(ctx, true)), nil
		}

		return newEmptyResult(), nil
	}

	var (
		shouldResult, filterResult, mustResult *queryResult
		err                                    error
	)

	if q.Parallel {
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

func (q *BoolQuery) runParallel(ctx context.Context) (*queryResult, *queryResult, *queryResult, error) {
	errors := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chShould := q.runAsyncQueries(ctx, q.Should, errors)
	chMust := q.runAsyncQueries(ctx, q.Must, errors)
	chFilter := q.runAsyncQueries(ctx, q.Filter, errors)

	var shouldResult *queryResult
	var filterResult *queryResult
	var mustResult *queryResult

	for i := 0; i < len(q.Should)+len(q.Must)+len(q.Filter); i++ {
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

func (q *BoolQuery) runAsyncQueries(ctx context.Context, queries []Query, errors chan<- error) <-chan *queryResult {
	ch := make(chan *queryResult)

	go func() {
		for _, query := range queries {
			go func(ctx context.Context, query Query) {
				res, err := query.Exec(ctx)
				if err != nil {
					errors <- err
				}
				ch <- res
			}(ctx, query)
		}
	}()

	return ch
}

func (q *BoolQuery) runSync(ctx context.Context) (*queryResult, *queryResult, *queryResult, error) {
	shouldResult, err := q.runSyncQueries(ctx, q.Should, func(src *queryResult, dst *queryResult) { src.Or(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	filterResult, err := q.runSyncQueries(ctx, q.Filter, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	mustResult, err := q.runSyncQueries(ctx, q.Must, func(src *queryResult, dst *queryResult) { src.And(dst) })
	if err != nil {
		return nil, nil, nil, err
	}

	return shouldResult, filterResult, mustResult, nil
}

func (q *BoolQuery) runSyncQueries(
	ctx context.Context,
	queries []Query,
	apply func(src *queryResult, dst *queryResult),
) (*queryResult, error) {
	var result *queryResult

	for _, query := range queries {
		r, err := query.Exec(ctx)
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
