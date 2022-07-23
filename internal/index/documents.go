package index

import (
	"context"
	"fmt"
	"time"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/query"
	"github.com/cyradin/search/internal/index/schema"
)

type DocSource map[string]interface{}

type Search struct {
	Query  map[string]interface{} `json:"query"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

type SearchResult struct {
	Took int        `json:"took"`
	Hits SearchHits `json:"hits"`
}

type SearchHits struct {
	Total    SearchTotal `json:"total"`
	Hits     []SearchHit `json:"hits"`
	MaxScore float64     `json:"maxScore"`
}

type SearchTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchHit struct {
	ID    uint32  `json:"id"`
	Score float64 `json:"score"`
}

type Documents struct {
	fields *field.Storage
}

func NewDocuments(dataPath string) *Documents {
	result := &Documents{
		fields: field.NewStorage(dataPath),
	}

	return result
}

func (d *Documents) AddIndex(index Index) error {
	_, err := d.fields.AddIndex(index.Name, index.Schema)
	if err != nil {
		return err
	}

	return nil
}

func (d *Documents) DeleteIndex(name string) error {
	d.fields.DeleteIndex(name)

	return nil
}

func (d *Documents) Add(index Index, id uint32, source DocSource) error {
	if id <= 0 {
		return fmt.Errorf("doc id is required")
	}

	if err := schema.ValidateDoc(index.Schema, source); err != nil {
		return fmt.Errorf("doc validation err: %w", err)
	}

	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return err
	}

	fieldIndex.Add(id, source)

	return nil
}

func (d *Documents) Get(index Index, id uint32) (DocSource, error) {
	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return nil, err
	}

	return fieldIndex.Get(id)
}

func (d *Documents) Delete(index Index, id uint32) error {
	if id <= 0 {
		return fmt.Errorf("doc id is required")
	}

	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return err
	}

	fieldIndex.Delete(id)

	return nil
}

func (d *Documents) Search(ctx context.Context, index Index, q Search) (SearchResult, error) {
	fieldIndex, err := d.fields.GetIndex(index.Name)
	if err != nil {
		return SearchResult{}, err
	}

	t := time.Now()
	result, err := query.Exec(ctx, q.Query, fieldIndex.Fields())
	took := time.Since(t).Microseconds()
	if err != nil {
		return SearchResult{}, err
	}

	hits := make([]SearchHit, len(result.Hits))
	for i, item := range result.Hits {
		hits[i] = SearchHit{
			ID:    item.ID,
			Score: item.Score,
		}
	}

	return SearchResult{
		Hits: SearchHits{
			Total: SearchTotal{
				Value:    result.Total.Value,
				Relation: result.Total.Relation,
			},
			Hits: hits,
		},
		Took: int(took),
	}, nil
}
