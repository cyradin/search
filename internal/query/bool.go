package query

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/entity"
)

func execBool(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil // @todo return ALL documents
	}

	var result *roaring.Bitmap
	for key, value := range data {
		path := pathJoin(path, key)
		v, ok := value.(entity.Query)
		if !ok {
			return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
		}

		var (
			bm  *roaring.Bitmap
			err error
		)

		switch key {
		case string(queryBoolShould):
			bm, err = execBoolShould(v, fields, path)
		case string(queryBoolMust):
			bm, err = execBoolMust(v, fields, path)
		case string(queryBoolFilter):
			bm, err = execBoolFilter(v, fields, path)
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

func execBoolShould(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for key, value := range data {
		path := pathJoin(path, key)
		v, ok := value.(entity.Query)
		if !ok {
			return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
		}

		bm, err := exec(v, fields, path)
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

func execBoolMust(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for key, value := range data {
		path := pathJoin(path, key)
		v, ok := value.(entity.Query)
		if !ok {
			return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
		}

		bm, err := exec(v, fields, path)
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

func execBoolFilter(data entity.Query, fields map[string]fieldValue, path string) (*roaring.Bitmap, error) {
	if len(data) == 0 {
		return roaring.New(), nil
	}

	var result *roaring.Bitmap
	for key, value := range data {
		path := pathJoin(path, key)
		v, ok := value.(entity.Query)
		if !ok {
			return nil, NewErrSyntax(errMsgObjectValueRequired(), path)
		}

		bm, err := exec(v, fields, path)
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
