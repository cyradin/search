package query

import (
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/entity"
)

func execTerm(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, val := firstVal(data)

	field, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}

	bm, ok := field.GetValue(val)
	if !ok {
		return roaring.New(), nil
	}

	return bm, nil
}

func execTerms(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, val := firstVal(data)

	var values []interface{}
	if reflect.TypeOf(val).Kind() == reflect.Slice {
		s := reflect.ValueOf(val)
		values = make([]interface{}, s.Len())
		for i := 0; i < s.Len(); i++ {
			values[i] = s.Index(i)
		}
	} else {
		return nil, NewErrSyntax(errMsgArrayValueRequired(), pathJoin(path, key))
	}

	field, ok := fields[key]
	if !ok {
		return roaring.New(), nil
	}

	bm, ok := field.GetValuesOr(values)
	if !ok {
		return roaring.New(), nil
	}

	return bm, nil
}
