package query

import (
	"context"

	"github.com/cyradin/search/internal/index/field"
	jsoniter "github.com/json-iterator/go"
)

type Query interface {
	Exec(ctx context.Context) (*queryResult, error)
}

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

type QueryRequest jsoniter.RawMessage
type Fields map[string]field.Field

func Exec(ctx context.Context, query QueryRequest, fields Fields) (Result, error) {
	ctx = withFields(ctx, fields)
	q, err := build(query)
	if err != nil {
		return Result{}, err
	}

	result, err := q.Exec(ctx)
	if err != nil {
		return Result{}, err
	}

	maxScore := 0.0
	hits := make([]Hit, 0, result.docs.GetCardinality())
	result.docs.Iterate(func(id uint32) bool {
		score := result.Score(id)
		if maxScore < score {
			maxScore = score
		}
		hits = append(hits, Hit{
			ID:    id,
			Score: score,
		})

		return true
	})

	return Result{
		Total: Total{
			Value:    int(result.docs.GetCardinality()),
			Relation: "eq",
		},
		Hits:     hits,
		MaxScore: maxScore,
	}, nil
}
