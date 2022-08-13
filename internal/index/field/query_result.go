package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
)

func DisableScoring(ctx context.Context) context.Context {
	return context.WithValue(ctx, "field.disable_scoring", true)
}

func IsScoringDisabled(ctx context.Context) bool {
	if v, ok := ctx.Value("field.disable_scoring").(bool); ok {
		return v
	}
	return false
}

const bm25K1 = 1.2
const bm25B = 0.75

type ResultOpt func(r *QueryResult)

type QueryResult struct {
	tokens          []string
	scoring         *Scoring
	scoringDisabled bool
	docs            *roaring.Bitmap
	boost           float64
	from            interface{}
	to              interface{}
}

func newResult(ctx context.Context, docs *roaring.Bitmap, opts ...ResultOpt) *QueryResult {
	result := &QueryResult{
		docs:  docs,
		boost: 1.0,
	}

	return result.WithOpts(ctx, opts...)
}

func newResultWithScoring(ctx context.Context, docs *roaring.Bitmap, scoring *Scoring, opts ...ResultOpt) *QueryResult {
	result := &QueryResult{
		docs:    docs,
		boost:   1.0,
		scoring: scoring,
	}

	return result.WithOpts(ctx, opts...)
}

func (r *QueryResult) WithOpts(ctx context.Context, opts ...ResultOpt) *QueryResult {
	if IsScoringDisabled(ctx) {
		opts = append(opts, WithDisabledScoring())
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *QueryResult) Docs() *roaring.Bitmap {
	return r.docs
}

func (r *QueryResult) Score(id uint32) float64 {
	if r.scoringDisabled {
		return 0
	}

	if !r.docs.Contains(id) {
		return 0
	}

	var score float64
	if len(r.tokens) == 0 {
		score = 1.0
	} else {
		for _, token := range r.tokens {
			score += r.scoring.BM25(id, bm25K1, bm25B, token)
		}
	}

	return score * r.boost
}

func (r *QueryResult) From() interface{} {
	return r.from
}

func (r *QueryResult) To() interface{} {
	return r.to
}

func WithFrom(from interface{}) ResultOpt {
	return func(r *QueryResult) {
		r.from = from
	}
}

func WithTo(to interface{}) ResultOpt {
	return func(r *QueryResult) {
		r.to = to
	}
}

func WithTokens(tokens []string) ResultOpt {
	return func(r *QueryResult) {
		r.tokens = tokens
	}
}

func WithBoost(boost float64) ResultOpt {
	return func(r *QueryResult) {
		r.boost = boost
	}
}

func WithDisabledScoring() ResultOpt {
	return func(r *QueryResult) {
		r.scoringDisabled = true
	}
}
