package query

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type SearchHit struct {
	ID uint32
}

type queryType string

const (
	queryTerm  queryType = "term"
	queryTerms queryType = "terms"

	queryBool       queryType = "bool"
	queryBoolShould queryType = "should"
	queryBoolMust   queryType = "must"
	queryBoolFilter queryType = "filter"
)

func queryTypes() []queryType {
	return []queryType{
		queryTerm,
		queryTerms,
		queryBool,
	}
}

type Query interface {
	exec(ctx context.Context) (*roaring.Bitmap, error)
}

type Req map[string]interface{}
type Fields map[string]field.Field

func Exec(ctx context.Context, req Req, fields Fields) ([]SearchHit, error) {
	ctx = withFields(ctx, fields)
	ctx = withPath(ctx, "query")
	q, err := build(ctx, req)
	if err != nil {
		return nil, err
	}

	bm, err := q.exec(ctx)
	if err != nil {
		return nil, err
	}

	hits := make([]SearchHit, 0, bm.GetCardinality())
	bm.Iterate(func(x uint32) bool {
		hits = append(hits, SearchHit{
			ID: x,
		})
		return true
	})

	return hits, nil
}

func build(ctx context.Context, req Req) (Query, error) {
	err := validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(errorRequired(ctx)),
		validation.Length(1, 1).ErrorObject(errorSingleKeyRequired(ctx)),
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
				return errorUnknownQueryType(ctx, key)
			}

			_, ok := val.(map[string]interface{})
			if !ok {
				return errorObjectRequired(ctx, key)
			}

			return nil
		}))
	if err != nil {
		return nil, err
	}

	key, value := firstVal(req)
	req = value.(map[string]interface{})
	ctx = withPath(ctx, path(ctx), key)

	qType := queryType(key)
	switch qType {
	case queryTerm:
		return newTermQuery(ctx, req)
	case queryTerms:
		return newTermsQuery(ctx, req)
	case queryBool:
		return newBoolQuery(ctx, req)
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
