package query

import "github.com/RoaringBitmap/roaring"

type fieldValue interface {
	GetValue(value interface{}) (*roaring.Bitmap, bool)
	GetValuesOr(values []interface{}) (*roaring.Bitmap, bool)
}

// var ErrEmptyQuery = fmt.Errorf("\"query\" field must not be empty")
// var ErrQuerySingleField = fmt.Errorf("\"query\" must have exactly one child field")

// // var ErrQuerySingleField = fmt.Errorf("\"query\" must have exactly one child field")

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
