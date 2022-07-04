package query

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Query = (*boolQuery)(nil)

type boolQuery struct {
	query  Req
	fields Fields
	path   string

	must   []Query
	should []Query
	filter []Query
}

func newBoolQuery(req Req, fields Fields, path string) (*boolQuery, error) {
	err := validation.Validate(req, validation.Map(
		validation.Key(string(queryBoolMust), validation.By(func(value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return validation.NewError("query_array_required", fmt.Sprintf("%q must be an array", queryBoolMust))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolShould), validation.By(func(value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return validation.NewError("query_array_required", fmt.Sprintf("%q must be an array", queryBoolShould))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolFilter), validation.By(func(value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return validation.NewError("query_array_required", fmt.Sprintf("%q must be an array", queryBoolFilter))
			}
			return nil
		})).Optional(),
	))
	if err != nil {
		return nil, err
	}

	result := &boolQuery{
		query:  req,
		fields: fields,
		path:   path,
	}

	for key, value := range req {
		path := pathJoin(path, key)

		values, _ := interfaceToSlice[map[string]interface{}](value)

		children := make([]Query, len(values))
		for i, v := range values {
			child, err := build(v, fields, path)
			if err != nil {
				return nil, err
			}
			children[i] = child
		}

		switch queryType(key) {
		case queryBoolShould:
			result.should = children
		case queryBoolMust:
			result.must = children
		case queryBoolFilter:
			result.filter = children
		}
	}

	return result, nil
}

func (q *boolQuery) exec() (*roaring.Bitmap, error) {
	if len(q.should) == 0 && len(q.must) == 0 && len(q.filter) == 0 {
		if ff, ok := q.fields[field.AllField]; ok {
			return ff.Get(true), nil
		}

		return roaring.New(), nil
	}

	var result *roaring.Bitmap

	for _, cq := range q.should {
		bm, err := cq.exec()
		if err != nil {
			return nil, err
		}

		if result == nil {
			result = bm
			continue
		}

		result.Or(bm)
	}

	for _, cq := range q.must {
		bm, err := cq.exec()
		if err != nil {
			return nil, err
		}

		if result == nil {
			result = bm
			continue
		}

		result.And(bm)
	}

	for _, cq := range q.filter {
		bm, err := cq.exec()
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
