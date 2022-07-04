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

type Query interface {
	exec() (*roaring.Bitmap, error)
}

type Req map[string]interface{}
type Fields map[string]field.Field

func Exec(data Req, fields Fields) ([]SearchHit, error) {
	q, err := build(data, fields, "query")
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

func build(query Req, fields Fields, path string) (Query, error) {
	if len(query) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(query) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, value := firstVal(query)

	val, ok := value.(map[string]interface{})
	if !ok {
		return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
	}

	query = val
	path = pathJoin(path, key)

	qType := queryType(key)
	switch qType {
	case queryTerm:
		return newTermQuery(query, fields, path)
	case queryTerms:
		return newTermsQuery(query, fields, path)
	case queryBool:
		return newBoolQuery(query, fields, path)
	}

	return nil, NewErrSyntax(errMsgOneOf(queryTypesString(), key), path)
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
