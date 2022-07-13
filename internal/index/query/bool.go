package query

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ internalQuery = (*boolQuery)(nil)

type boolQuery struct {
	query Query

	must   []internalQuery
	should []internalQuery
	filter []internalQuery
}

func newBoolQuery(ctx context.Context, query Query) (*boolQuery, error) {
	err := validation.ValidateWithContext(ctx, query, validation.Map(
		validation.Key(string(queryBoolMust), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolMust))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolShould), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolShould))
			}
			return nil
		})).Optional(),
		validation.Key(string(queryBoolFilter), validation.WithContext(func(ctx context.Context, value interface{}) error {
			_, err := interfaceToSlice[map[string]interface{}](value)
			if err != nil {
				return errs.ArrayRequired(ctx, string(queryBoolFilter))
			}
			return nil
		})).Optional(),
	))
	if err != nil {
		return nil, err
	}

	result := &boolQuery{
		query: query,
	}

	for key, value := range query {
		values, _ := interfaceToSlice[map[string]interface{}](value)

		children := make([]internalQuery, len(values))
		for i, v := range values {
			ctx := errs.WithPath(ctx, errs.Path(ctx), key)
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

func (q *boolQuery) exec(ctx context.Context) (queryResult, error) {
	fields := fields(ctx)

	if len(q.should) == 0 && len(q.must) == 0 && len(q.filter) == 0 {
		if ff, ok := fields[field.AllField]; ok {
			return newNoScoreResult(ff.Get(true)), nil
		}

		return newEmptyResult(), nil
	}

	var v *roaring.Bitmap
	for _, data := range []struct {
		queries []internalQuery
		apply   func(src *roaring.Bitmap, bm *roaring.Bitmap)
	}{
		{queries: q.filter, apply: func(src *roaring.Bitmap, bm *roaring.Bitmap) { src.And(bm) }},
		{queries: q.must, apply: func(src *roaring.Bitmap, bm *roaring.Bitmap) { src.And(bm) }},
		{queries: q.should, apply: func(src *roaring.Bitmap, bm *roaring.Bitmap) { src.Or(bm) }},
	} {
		for _, q := range data.queries {
			r, err := q.exec(ctx)
			if err != nil {
				return newEmptyResult(), err
			}

			if v == nil {
				v = r.bm
				continue
			}

			data.apply(v, r.bm)
		}
	}

	return newNoScoreResult(v), nil
}
