package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/valid"
	jsoniter "github.com/json-iterator/go"
)

type Agg interface {
	Exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error)
}

const AggsKey = "aggs"

type Result map[string]interface{}
type AggsRequest map[string]jsoniter.RawMessage

func Exec(ctx context.Context, docs *roaring.Bitmap, req AggsRequest, fields Fields) (Result, error) {
	if docs == nil {
		docs = roaring.New()
	}

	ctx = withFields(ctx, fields)
	ctx = valid.WithPath(ctx, AggsKey)

	aggs, err := build(req)
	if err != nil {
		return nil, err
	}

	result := make(Result, len(aggs))
	for key, agg := range aggs {
		r, err := agg.Exec(ctx, docs)
		if err != nil {
			return nil, err
		}
		result[key] = r
	}

	return result, nil
}
