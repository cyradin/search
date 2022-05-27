package query

import (
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
)

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

func Exec(q map[string]interface{}, fields map[string]field.Field) ([]entity.SearchHit, error) {
	bm, err := exec(q, fields, "query")
	if err != nil {
		return nil, err
	}

	hits := make([]entity.SearchHit, 0, bm.GetCardinality())
	bm.Iterate(func(x uint32) bool {
		hits = append(hits, entity.SearchHit{
			ID: x,
		})
		return true
	})

	return hits, nil
}

func exec(q map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(q) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(q) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, value := firstVal(q)

	val, ok := value.(map[string]interface{})
	if !ok {
		return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
	}

	qType := queryType(key)
	qPath := pathJoin(path, key)
	switch qType {
	case queryTerm:
		return execTerm(val, fields, qPath)
	case queryTerms:
		return execTerms(val, fields, qPath)
	case queryBool:
		return execBool(val, fields, qPath)
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
