package query

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
)

var _ query = (*boolQuery)(nil)

type boolQuery struct {
	params queryParams

	must   []query
	should []query
	filter []query
}

func newBoolQuery(params queryParams) (*boolQuery, error) {
	result := &boolQuery{
		params: params,
	}

	for key, value := range params.data {
		path := pathJoin(params.path, key)

		values, err := interfaceToSlice[map[string]interface{}](value)
		if err != nil {
			return nil, NewErrSyntax(errMsgArrayValueRequired(), pathJoin(path, key))
		}

		qType := queryType(key)
		if qType != queryBoolShould && qType != queryBoolMust && qType != queryBoolFilter {
			return nil, NewErrSyntax(
				errMsgOneOf([]string{string(queryBoolShould), string(queryBoolMust), string(queryBoolFilter)}, key),
				params.path,
			)
		}

		children := make([]query, len(values))
		for i, v := range values {
			params := queryParams{
				fields: params.fields,
				data:   v,
				path:   path,
			}

			child, err := build(params)
			if err != nil {
				return nil, err
			}
			children[i] = child
		}

		switch qType {
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
		if ff, ok := q.params.fields[field.AllField]; ok {
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
