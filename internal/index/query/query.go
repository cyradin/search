package query

import (
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
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

func queryTypesString() []string {
	types := queryTypes()
	result := make([]string, len(types))
	for i, qt := range types {
		result[i] = string(qt)
	}
	return result
}

type query interface {
	exec() (*roaring.Bitmap, error)
}

type queryParams struct {
	data   map[string]interface{}
	fields map[string]field.Field
	path   string
}

func Exec(data map[string]interface{}, fields map[string]field.Field) ([]SearchHit, error) {
	q, err := build(queryParams{
		data:   data,
		fields: fields,
		path:   "query",
	})
	if err != nil {
		return nil, err
	}

	bm, err := q.exec()
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

func build(params queryParams) (query, error) {
	if len(params.data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), params.path)
	}
	if len(params.data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), params.path)
	}

	key, value := firstVal(params.data)

	val, ok := value.(map[string]interface{})
	if !ok {
		return nil, NewErrSyntax(errMsgObjectValueRequired(), params.path)
	}

	params.data = val
	params.path = pathJoin(params.path, key)

	qType := queryType(key)
	switch qType {
	case queryTerm:
		return newTermQuery(params)
	case queryTerms:
		return newTermsQuery(params)
	case queryBool:
		return newBoolQuery(params)
	}

	return nil, NewErrSyntax(errMsgOneOf(queryTypesString(), key), params.path)
}

func firstVal(m map[string]interface{}) (string, interface{}) {
	for k, v := range m {
		return k, v
	}

	return "", nil
}

func pathJoin(path string, value string) string {
	return path + "." + value
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
