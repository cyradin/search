package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Query = (*boolQuery)(nil)

type boolQuery struct {
	query Req

	must   []Query
	should []Query
	filter []Query
}

func newBoolQuery(ctx context.Context, req Req) (*boolQuery, error) {
	err := validation.ValidateWithContext(ctx, req, validation.Map(
		validation.Key(string(queryBoolMust), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errorArrayRequired(ctx, string(queryBoolMust))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolShould), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errorArrayRequired(ctx, string(queryBoolShould))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolFilter), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errorArrayRequired(ctx, string(queryBoolFilter))
			}
			return nil
		})).Optional(),
	))
	if err != nil {
		return nil, err
	}

	result := &boolQuery{
		query: req,
	}

	for key, value := range req {
		values, _ := interfaceToSlice[map[string]interface{}](value)

		children := make([]Query, len(values))
		for i, v := range values {
			ctx := withPath(ctx, path(ctx), key)
			child, err := build(ctx, v)
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

func (q *boolQuery) exec(ctx context.Context) (*roaring.Bitmap, error) {
	fields := fields(ctx)

	if len(q.should) == 0 && len(q.must) == 0 && len(q.filter) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			return ff.Get(true), nil
		}

		return roaring.New(), nil
	}

	var result *roaring.Bitmap

	for _, cq := range q.should {
		bm, err := cq.exec(ctx)
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
		bm, err := cq.exec(ctx)
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
		bm, err := cq.exec(ctx)
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
