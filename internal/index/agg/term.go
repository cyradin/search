package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
)

var _ internalAgg = (*termAgg)(nil)

type termAgg struct {
	req Aggs
}

func newTermAgg(ctx context.Context, req Aggs) (*termAgg, error) {
	// @todo perform validation
	return &termAgg{
		req: req,
	}, nil
}

func (a *termAgg) exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error) {
	return nil, nil // @todo
}
