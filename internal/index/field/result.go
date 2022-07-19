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

type ResultOpt func(r *Result)

type Result struct {
	tokens          []string
	scoring         *Scoring
	scoringDisabled bool
	docs            *roaring.Bitmap
	boost           float64
}

func NewResult(ctx context.Context, docs *roaring.Bitmap, opts ...ResultOpt) *Result {
	result := &Result{
		docs:  docs,
		boost: 1.0,
	}

	return result.WithOpts(ctx, opts...)
}

func NewResultWithScoring(ctx context.Context, docs *roaring.Bitmap, scoring *Scoring, opts ...ResultOpt) *Result {
	result := &Result{
		docs:    docs,
		boost:   1.0,
		scoring: scoring,
	}

	return result.WithOpts(ctx, opts...)
}

func (r *Result) WithOpts(ctx context.Context, opts ...ResultOpt) *Result {
	if IsScoringDisabled(ctx) {
		opts = append(opts, WithDisabledScoring())
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *Result) Docs() *roaring.Bitmap {
	return r.docs
}

func (r *Result) Score(id uint32) float64 {
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

func WithTokens(tokens []string) ResultOpt {
	return func(r *Result) {
		r.tokens = tokens
	}
}

func WithBoost(boost float64) ResultOpt {
	return func(r *Result) {
		r.boost = boost
	}
}

func WithDisabledScoring() ResultOpt {
	return func(r *Result) {
		r.scoringDisabled = true
	}
}
