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
		v, err := Build(QueryRequest(r))
		if err != nil {
			return err
		}
		q.Must[i] = v
	}

	q.Should = make([]Query, len(d.Should))
	for i, r := range d.Should {
		v, err := Build(QueryRequest(r))
		if err != nil {
			return err
		}
		q.Should[i] = v
	}

	q.Filter = make([]Query, len(d.Filter))
	for i, r := range d.Filter {
		v, err := Build(QueryRequest(r))
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

func (q *BoolQuery) Exec(ctx context.Context, fields Fields) (Result, error) {
	if len(q.Should) == 0 && len(q.Must) == 0 && len(q.Filter) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			return NewResult(ff.TermQuery(ctx, true)), nil
		}

		return NewEmptyResult(), nil
	}

	var (
		shouldResult, filterResult, mustResult Result
		err                                    error
	)

	if q.Parallel {
		shouldResult, filterResult, mustResult, err = q.runParallel(ctx, fields)
	} else {
		shouldResult, filterResult, mustResult, err = q.runSync(ctx, fields)
	}
	if err != nil {
		return NewEmptyResult(), err
	}

	var result Result
	if !shouldResult.IsEmpty() {
		result = shouldResult
	}
	if !mustResult.IsEmpty() {
		if result.IsEmpty() {
			result = mustResult
		} else {
			result.And(mustResult)
		}
	}
	if !filterResult.IsEmpty() {
		if result.IsEmpty() {
			result = filterResult
		} else {
			result.And(filterResult)
		}
	}

	if result.IsEmpty() {
		return NewEmptyResult(), nil
	}

	return result, nil
}

func (q *BoolQuery) runParallel(ctx context.Context, fields Fields) (shouldResult Result, filterResult Result, mustResult Result, err error) {
	errors := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chShould := q.runAsyncQueries(ctx, fields, q.Should, errors)
	chMust := q.runAsyncQueries(ctx, fields, q.Must, errors)
	chFilter := q.runAsyncQueries(ctx, fields, q.Filter, errors)

	for i := 0; i < len(q.Should)+len(q.Must)+len(q.Filter); i++ {
		select {
		case r := <-chShould:
			if shouldResult.IsEmpty() {
				shouldResult = r
			} else {
				shouldResult.Or(r)
			}
		case r := <-chMust:
			if mustResult.IsEmpty() {
				mustResult = r
			} else {
				mustResult.And(r)
			}
		case r := <-chFilter:
			if filterResult.IsEmpty() {
				filterResult = r
			} else {
				filterResult.And(r)
			}
		case e := <-errors:
			err = e
			return
		}
	}

	return shouldResult, filterResult, mustResult, nil

}

func (q *BoolQuery) runAsyncQueries(ctx context.Context, fields Fields, queries []Query, errors chan<- error) <-chan Result {
	ch := make(chan Result)

	go func() {
		for _, query := range queries {
			go func(ctx context.Context, query Query) {
				res, err := query.Exec(ctx, fields)
				if err != nil {
					errors <- err
				}
				ch <- res
			}(ctx, query)
		}
	}()

	return ch
}

func (q *BoolQuery) runSync(ctx context.Context, fields Fields) (shouldResult Result, filterResult Result, mustResult Result, err error) {
	shouldResult, err = q.runSyncQueries(ctx, fields, q.Should, func(src Result, dst Result) { src.Or(dst) })
	if err != nil {
		return
	}

	filterResult, err = q.runSyncQueries(ctx, fields, q.Filter, func(src Result, dst Result) { src.And(dst) })
	if err != nil {
		return
	}

	mustResult, err = q.runSyncQueries(ctx, fields, q.Must, func(src Result, dst Result) { src.And(dst) })
	return
}

func (q *BoolQuery) runSyncQueries(
	ctx context.Context,
	fields Fields,
	queries []Query,
	apply func(src Result, dst Result),
) (Result, error) {
	var result Result

	for _, query := range queries {
		r, err := query.Exec(ctx, fields)
		if err != nil {
			return NewEmptyResult(), err
		}

		if result.IsEmpty() {
			result = r
			continue
		}

		apply(result, r)
	}

	return result, nil
}
