package query

import (
	"context"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type queryType string

const (
	queryTerm  queryType = "term"
	queryTerms queryType = "terms"

	queryBool       queryType = "bool"
	queryBoolShould queryType = "should"
	queryBoolMust   queryType = "must"
	queryBoolFilter queryType = "filter"

	queryMatch queryType = "match"

	queryRange queryType = "range"
)

func queryTypes() []queryType {
	return []queryType{
		queryTerm,
		queryTerms,
		queryBool,
		queryMatch,
		queryRange,
	}
}

type queryResult struct {
	docs    *roaring.Bitmap
	results []*field.Result
}

func newEmptyResult() *queryResult {
	return &queryResult{
		docs: roaring.New(),
	}
}

func newResult(res *field.Result) *queryResult {
	return &queryResult{
		docs:    res.Docs(),
		results: []*field.Result{res},
	}
}

func (r *queryResult) And(res *queryResult) {
	r.docs.And(res.Docs())
	r.results = append(r.results, res.results...)
}

func (r *queryResult) Or(res *queryResult) {
	r.docs.Or(res.Docs())
	r.results = append(r.results, res.results...)
}

func (r *queryResult) Docs() *roaring.Bitmap {
	return r.docs
}

func (r *queryResult) Score(id uint32) float64 {
	if !r.docs.Contains(id) {
		return 0
	}

	result := 0.0
	for _, res := range r.results {
		result += res.Score(id)
	}

	return result
}

type internalQuery interface {
	exec(ctx context.Context) (*queryResult, error)
}

func build(ctx context.Context, req Query) (internalQuery, error) {
	err := validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(errs.Required(ctx)),
		validation.Length(1, 1).ErrorObject(errs.SingleKeyRequired(ctx)),
		validation.WithContext(func(ctx context.Context, value interface{}) error {
			key, val := firstVal(req)
			var querytypeValid bool
			for _, qt := range queryTypes() {
				if key == string(qt) {
					querytypeValid = true
					break
				}
			}
			if !querytypeValid {
				return errs.UnknownValue(ctx, key)
			}

			_, ok := val.(map[string]interface{})
			if !ok {
				return errs.ObjectRequired(ctx, key)
			}

			return nil
		}))
	if err != nil {
		return nil, err
	}

	key, value := firstVal(req)
	req = value.(map[string]interface{})
	ctx = errs.WithPath(ctx, errs.Path(ctx), key)

	qType := queryType(key)
	switch qType {
	case queryTerm:
		return newTermQuery(ctx, req)
	case queryTerms:
		return newTermsQuery(ctx, req)
	case queryBool:
		return newBoolQuery(ctx, req)
	case queryMatch:
		return newMatchQuery(ctx, req)
	case queryRange:
		return newRangeQuery(ctx, req)
	}

	// must not be executed because of validation made earlier
	panic(errs.Errorf("unknown query type %q", key))
}

func firstVal(m map[string]interface{}) (string, interface{}) {
	for k, v := range m {
		return k, v
	}

	return "", nil
}

func interfaceToSlice[T any](value interface{}) ([]T, error) {
	if reflect.TypeOf(value).Kind() != reflect.Slice {
		return nil, errs.Errorf("value is not a slice")
	}

	s := reflect.ValueOf(value)
	result := make([]T, s.Len())
	for i := 0; i < s.Len(); i++ {
		val := s.Index(i).Interface()
		vv, ok := val.(T)
		if !ok {
			tt := new(T)
			return nil, errs.Errorf("invalid #%d element value: required %#v, got %#v", i, tt, val)
		}

		result[i] = vv
	}

	return result, nil
}
