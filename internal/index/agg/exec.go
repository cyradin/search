package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/valid"
)

const AggsKey = "aggs"

type Result map[string]interface{}
type Aggs map[string]interface{}

func Exec(ctx context.Context, docs *roaring.Bitmap, req Aggs, fields Fields) (Result, error) {
	if docs == nil {
		docs = roaring.New()
	}

	ctx = withFields(ctx, fields)
	ctx = valid.WithPath(ctx, AggsKey)

	aggs, err := build(ctx, req)
	if err != nil {
		return nil, err
	}

	result := make(Result, len(aggs))
	for key, agg := range aggs {
		r, err := agg.exec(ctx, docs)
		if err != nil {
			return nil, err
		}
		result[key] = r
	}

	return result, nil
}
