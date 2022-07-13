package query

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type Result struct {
	Hits     []Hit
	Total    Total
	MaxScore float64
}

type Total struct {
	Value    int
	Relation string
}

type Hit struct {
	ID    uint32
	Score float64
}

type queryType string

const (
	queryTerm  queryType = "term"
	queryTerms queryType = "terms"

	queryBool       queryType = "bool"
	queryBoolShould queryType = "should"
	queryBoolMust   queryType = "must"
	queryBoolFilter queryType = "filter"

	queryMatch queryType = "match"
)

func queryTypes() []queryType {
	return []queryType{
		queryTerm,
		queryTerms,
		queryBool,
	}
}

type internalQuery interface {
	exec(ctx context.Context) (*roaring.Bitmap, error)
}

type Query map[string]interface{}
type Fields map[string]field.Field

func Exec(ctx context.Context, query Query, fields Fields) (Result, error) {
	ctx = withFields(ctx, fields)
	ctx = errs.WithPath(ctx, "query")
	q, err := build(ctx, query)
	if err != nil {
		return Result{}, err
	}

	bm, err := q.exec(ctx)
	if err != nil {
		return Result{}, err
	}

	hits := make([]Hit, 0, bm.GetCardinality())
	bm.Iterate(func(x uint32) bool {
		hits = append(hits, Hit{
			ID:    x,
			Score: 0, // @todo
		})
		return true
	})

	return Result{
		Total: Total{
			Value:    int(bm.GetCardinality()),
			Relation: "eq",
		},
		Hits:     hits,
		MaxScore: 0, // @todo
	}, nil
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
	}

	// must not be executed because of validation made earlier
	panic(fmt.Errorf("unknown query type %q", key))
}

func firstVal(m map[string]interface{}) (string, interface{}) {
	for k, v := range m {
		return k, v
	}

	return "", nil
}

func interfaceToSlice[T any](value interface{}) ([]T, error) {
	if reflect.TypeOf(value).Kind() != reflect.Slice {
		return nil, fmt.Errorf("value is not a slice")
	}

	s := reflect.ValueOf(value)
	result := make([]T, s.Len())
	for i := 0; i < s.Len(); i++ {
		val := s.Index(i).Interface()
		vv, ok := val.(T)
		if !ok {
			tt := new(T)
			return nil, fmt.Errorf("invalid #%d element value: required %#v, got %#v", i, tt, val)
		}

		result[i] = vv
	}

	return result, nil
}
