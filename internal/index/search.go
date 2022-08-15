package index

import (
	"context"
	"fmt"
	"time"

	"github.com/cyradin/search/internal/index/agg"
	"github.com/cyradin/search/internal/index/field"
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
func (d *Documents) Search(ctx context.Context, index Index, q Search) (SearchResult, error) {
	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return SearchResult{}, err
	}
	fields := fieldIndex.Fields()

	t := time.Now()
	qr, err := d.execQuery(ctx, q, fields)
	if err != nil {
		return SearchResult{}, err
	}

	fmt.Println(qr.Docs().ToArray())

	ar, err := d.execAggs(ctx, q, qr, fields)
	if err != nil {
		return SearchResult{}, err
	}
	took := time.Since(t).Microseconds()

	return NewSearchResult(qr, ar, took), nil
}

func (d *Documents) execQuery(ctx context.Context, q Search, fields map[string]field.Field) (query.Result, error) {
	// exec query by all documents if not provided
	if q.Query == nil {
		q.Query = []byte(`{"type": "bool"}`)
	}

	qb, err := query.Build(query.QueryRequest(q.Query))
	if err != nil {
		return query.NewEmptyResult(), err
	}

	qr, err := qb.Exec(ctx, fields)
	if err != nil {
		return query.NewEmptyResult(), err
	}

	return qr, nil
}

func (d *Documents) execAggs(ctx context.Context, q Search, qr query.Result, fields map[string]field.Field) (agg.Result, error) {
	return agg.Exec(ctx, qr.Docs(), agg.AggsRequest(q.Aggs), fields)
}

type SearchResult struct {
	Took int64      `json:"took"`
	Hits SearchHits `json:"hits"`
}

func NewSearchResult(qr query.Result, ar agg.Result, took int64) SearchResult {
	return SearchResult{
		Hits: NewSearchHits(qr),
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
