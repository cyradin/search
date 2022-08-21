package index

import (
	"context"
	"time"

	"github.com/cyradin/search/internal/index/agg"
	"github.com/cyradin/search/internal/index/query"
	jsoniter "github.com/json-iterator/go"
)

// Search search request
type Search struct {
	Query  jsoniter.RawMessage            `json:"query"`
	Aggs   map[string]jsoniter.RawMessage `json:"aggs"`
	Limit  int                            `json:"limit"`
	Offset int                            `json:"offset"`
}

// Search execute search
func (i *Index) Search(ctx context.Context, q Search) (SearchResult, error) {
	t := time.Now()
	qr, err := i.execQuery(ctx, q)
	if err != nil {
		return SearchResult{}, err
	}

	ar, err := i.execAggs(ctx, q, qr)
	if err != nil {
		return SearchResult{}, err
	}
	took := time.Since(t).Microseconds()

	return NewSearchResult(qr, ar, took), nil
}

func (i *Index) execQuery(ctx context.Context, q Search) (query.Result, error) {
	// exec query by all documents if not provided
	if q.Query == nil {
		q.Query = []byte(`{"type": "bool"}`)
	}

	qb, err := query.Build(query.QueryRequest(q.Query))
	if err != nil {
		return query.NewEmptyResult(), err
	}

	qr, err := qb.Exec(ctx, i.fields)
	if err != nil {
		return query.NewEmptyResult(), err
	}

	return qr, nil
}

func (i *Index) execAggs(ctx context.Context, q Search, qr query.Result) (agg.Result, error) {
	return agg.Exec(ctx, qr.Docs(), agg.AggsRequest(q.Aggs), i.fields)
}

type SearchResult struct {
	Took int64                  `json:"took"`
	Hits SearchHits             `json:"hits"`
	Aggs map[string]interface{} `json:"aggs"`
}

func NewSearchResult(qr query.Result, ar agg.Result, took int64) SearchResult {
	return SearchResult{
		Hits: NewSearchHits(qr),
		Aggs: ar,
		Took: took,
	}
}

type SearchHits struct {
	Total    SearchTotal `json:"total"`
	Hits     []SearchHit `json:"hits"`
	MaxScore float64     `json:"maxScore"`
}

func NewSearchHits(qr query.Result) SearchHits {
	total := qr.Docs().GetCardinality()
	hits := make([]SearchHit, total)
	maxScore := 0.0
	for i, id := range qr.Docs().ToArray() {
		score := qr.Score(id)
		if maxScore < score {
			maxScore = score
		}
		hits[i] = SearchHit{
			ID:    id,
			Score: score,
		}
	}

	return SearchHits{
		Total: SearchTotal{
			Value:    int(total),
			Relation: "eq",
		},
		Hits:     hits,
		MaxScore: maxScore,
	}
}

type SearchTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchHit struct {
	ID    uint32  `json:"id"`
	Score float64 `json:"score"`
}
