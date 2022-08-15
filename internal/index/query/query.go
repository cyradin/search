package query

import (
	"bytes"
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/field"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
)

type Query interface {
	Exec(ctx context.Context, fields Fields) (Result, error)
}

type Total struct {
	Value    int
	Relation string
}

type QueryRequest jsoniter.RawMessage
type Fields map[string]field.Field

type Result struct {
	docs    *roaring.Bitmap
	results []*field.QueryResult
}

func (r Result) IsEmpty() bool {
	return len(r.results) == 0
}

func NewEmptyResult() Result {
	return Result{
		docs: roaring.New(),
	}
}

func NewResult(res *field.QueryResult) Result {
	return Result{
		docs:    res.Docs(),
		results: []*field.QueryResult{res},
	}
}

func (r *Result) And(res Result) {
	r.docs.And(res.Docs())
	r.results = append(r.results, res.results...)
}

func (r *Result) Or(res Result) {
	r.docs.Or(res.Docs())
	r.results = append(r.results, res.results...)
}

func (r *Result) Docs() *roaring.Bitmap {
	return r.docs
}

func (r *Result) Score(id uint32) float64 {
	if !r.docs.Contains(id) {
		return 0
	}

	result := 0.0
	for _, res := range r.results {
		result += res.Score(id)
	}

	return result
}

type QueryType struct {
	Type string `json:"type"`
}

func Build(req QueryRequest) (Query, error) {
	queryType := new(QueryType)

	dec := jsoniter.NewDecoder(bytes.NewBuffer(req))
	dec.UseNumber()
	err := dec.Decode(queryType)
	if err != nil {
		return nil, err
	}

	var query Query
	switch queryType.Type {
	case "term":
		query = new(TermQuery)
	case "terms":
		query = new(TermsQuery)
	case "bool":
		query = new(BoolQuery)
	case "match":
		query = new(MatchQuery)
	case "range":
		query = new(RangeQuery)
	default:
		return nil, fmt.Errorf("unknown query type %q", queryType.Type)
	}

	dec = jsoniter.NewDecoder(bytes.NewBuffer(req))
	dec.UseNumber()
	err = dec.Decode(query)
	if err != nil {
		return nil, err
	}

	err = validation.Validate(query)
	if err != nil {
		return nil, err
	}

	return query, nil
}
