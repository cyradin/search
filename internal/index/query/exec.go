package query

import (
	"context"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/field"
)

type Result struct {
	Hits     []Hit
	Total    Total
	MaxScore float64
}

type Total struct {
	Value    int
	Relation string
}

type Hit struct {
	ID    uint32
	Score float64
}

type Query map[string]interface{}
type Fields map[string]field.Field

func Exec(ctx context.Context, query Query, fields Fields) (Result, error) {
	ctx = withFields(ctx, fields)
	ctx = errs.WithPath(ctx, "query")
	q, err := build(ctx, query)
	if err != nil {
		return Result{}, err
	}

	result, err := q.exec(ctx)
	if err != nil {
		return Result{}, err
	}

	maxScore := 0.0
	hits := make([]Hit, 0, result.bm.GetCardinality())
	result.bm.Iterate(func(id uint32) bool {
		score := result.scores[id]
		if maxScore < score {
			maxScore = score
		}
		hits = append(hits, Hit{
			ID:    id,
			Score: result.scores[id],
		})

		return true
	})

	return Result{
		Total: Total{
			Value:    int(result.bm.GetCardinality()),
			Relation: "eq",
		},
		Hits:     hits,
		MaxScore: maxScore,
	}, nil
}
