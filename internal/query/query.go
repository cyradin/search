package query

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/entity"
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

type fieldValue interface {
	GetValue(value interface{}) (*roaring.Bitmap, bool)
	GetValuesOr(values []interface{}) (*roaring.Bitmap, bool)
}

func exec(q entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(q) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(q) > 0 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, value := firstVal(q)

	val, ok := value.(entity.Query)
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

// func Exec(q entity.Query, fields map[string]field.Field) ([]entity.SearchHit, error) {
// 	if len(q) == 0 {
// 		return nil, ErrEmptyQuery
// 	}

// 	if len(q) > 1 {
// 		return nil, ErrEmptyQuery
// 	}

// 	var bm *roaring.Bitmap
// 	var err error
// 	for k, v := range q {
// 		switch q[k] {
// 		case "term":
// 			bm, err = term(v, fields)
// 			if err != nil {
// 				return nil, err
// 			}
// 		default:

// 		}
// 	}

// }

func firstVal(m map[string]interface{}) (string, interface{}) {
	for k, v := range m {
		return k, v
	}

	return "", nil
}

func pathJoin(path string, value string) string {
	return path + "." + value
}
