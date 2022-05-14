package query

import (
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
