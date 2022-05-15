package query

import (
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

func term(m map[string]interface{}, fields map[string]fieldValue) (*roaring.Bitmap, error) {
	if len(m) == 0 {
		return nil, NewErrSyntax("term query cannot be empty")
	}
	if len(m) > 1 {
		return nil, NewErrSyntax("term query cannot have multiple fields")
	}

	k, v := firstVal(m)

	f, ok := fields[k]
	if !ok {
		return roaring.New(), nil
	}

	bm, ok := f.GetValue(v)
	if !ok {
		return roaring.New(), nil
	}

	return bm, nil
}

func terms(m map[string]interface{}, fields map[string]fieldValue) (*roaring.Bitmap, error) {
	if len(m) == 0 {
		return nil, NewErrSyntax("terms query cannot be empty")
	}
	if len(m) > 1 {
		return nil, NewErrSyntax("terms query cannot have multiple fields")
	}

	k, v := firstVal(m)

	var values []interface{}
	if reflect.TypeOf(v).Kind() == reflect.Slice {
		s := reflect.ValueOf(v)
		values = make([]interface{}, s.Len())
		for i := 0; i < s.Len(); i++ {
			values[i] = s.Index(i)
		}
	} else {
		return nil, NewErrSyntax("terms query values must be an array")
	}

	f, ok := fields[k]
	if !ok {
		return roaring.New(), nil
	}

	bm, ok := f.GetValuesOr(values)
	if !ok {
		return roaring.New(), nil
	}

	return bm, nil
}
