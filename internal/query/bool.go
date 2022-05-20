package query

import (
	"strconv"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
)

func execBool(data map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			if all, ok := ff.GetValue(true); ok {
				return all, nil
			}
		}

		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for key, value := range data {
		path := pathJoin(path, key)

		values, err := interfaceToSlice[map[string]interface{}](value)
		if err != nil {
			return nil, NewErrSyntax(errMsgArrayValueRequired(), pathJoin(path, key))
		}

		var (
			bm *roaring.Bitmap
		)

		switch key {
		case string(queryBoolShould):
			bm, err = execBoolShould(values, fields, path)
		case string(queryBoolMust):
			bm, err = execBoolMust(values, fields, path)
		case string(queryBoolFilter):
			bm, err = execBoolFilter(values, fields, path)
		default:
			return nil, NewErrSyntax(
				errMsgOneOf([]string{string(queryBoolShould), string(queryBoolMust), string(queryBoolFilter)}, key),
				path,
			)
		}

		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
			continue
		}

		result.And(bm)
	}

	return result, nil
}

func execBoolShould(data []map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for i, value := range data {
		path := pathJoin(path, strconv.Itoa(i))
		bm, err := exec(value, fields, path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
			continue
		}

		result.Or(bm)
	}

	if result == nil {
		return roaring.New(), nil
	}

	return result, nil
}

func execBoolMust(data []map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for i, value := range data {
		path := pathJoin(path, strconv.Itoa(i))
		bm, err := exec(value, fields, path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
			continue
		}

		result.And(bm)
	}

	if result == nil {
		return roaring.New(), nil
	}

	return result, nil
}

func execBoolFilter(data []map[string]interface{}, fields map[string]field.Field, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for i, value := range data {
		path := pathJoin(path, strconv.Itoa(i))
		bm, err := exec(value, fields, path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = bm
			continue
		}

		result.And(bm)
	}

	if result == nil {
		return roaring.New(), nil
	}

	return result, nil
}
