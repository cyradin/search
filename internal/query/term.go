package query

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
)

func execTerm(data map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
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

func execTerms(data map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	key, val := firstVal(data)

	values, err := interfaceToSlice[interface{}](val)
	if err != nil {
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
