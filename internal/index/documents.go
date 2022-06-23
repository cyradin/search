package index

import (
	"fmt"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/query"
	"github.com/cyradin/search/internal/index/relevance"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/index/source"
)

type Documents struct {
	sources   *source.Storage
	fields    *field.Storage
	relevance *relevance.Storage
}

func NewDocuments(dataPath string) *Documents {
	result := &Documents{
		relevance: relevance.NewStorage(dataPath),
		fields:    field.NewStorage(dataPath),
		sources:   source.NewStorage(dataPath),
	}

	return result
}

func (d *Documents) AddIndex(index entity.Index) error {
	_, err := d.sources.AddIndex(index.Name)
	if err != nil {
		return err
	}
	_, err = d.fields.AddIndex(index.Name, index.Schema)
	if err != nil {
		return err
	}

	_, err = d.relevance.AddIndex(index.Name)
	if err != nil {
		return err
	}

	return nil
}

func (d *Documents) Add(index entity.Index, id uint32, source entity.DocSource) (uint32, error) {
	if err := schema.ValidateDoc(index.Schema, source); err != nil {
		return 0, fmt.Errorf("source validation err: %w", err)
	}

	srcIndex, fieldIndex, _, err := d.getIndexes(index.Name)
	if err != nil {
		return 0, err
	}

	id, err = srcIndex.Insert(id, source)
	if err != nil {
		return id, fmt.Errorf("source insert err: %w", err)
	}
	fieldIndex.Add(id, source)

	return id, nil
}

func (d *Documents) Get(index entity.Index, id uint32) (entity.DocSource, error) {
	srcIndex, _, _, err := d.getIndexes(index.Name)
	if err != nil {
		return nil, err
	}

	doc, err := srcIndex.One(id)
	if err != nil {
		return nil, fmt.Errorf("source get err: %w", err)
	}

	return doc.Source, err
}

func (d *Documents) Search(index entity.Index, q entity.Search) (entity.SearchResult, error) {
	_, fieldIndex, _, err := d.getIndexes(index.Name)
	if err != nil {
		return entity.SearchResult{}, err
	}

	hits, err := query.Exec(q.Query, fieldIndex.Fields())
	if err != nil {
		return entity.SearchResult{}, err
	}

	fmt.Println(hits) // @todo make search result

	return entity.SearchResult{}, nil
}

func (d *Documents) getIndexes(
	name string,
) (sourceIndex *source.Index, fieldIndex *field.Index, relevanceIndex *relevance.Index, err error) {
	sourceIndex, err = d.sources.GetIndex(name)
	if err != nil {
		return
	}
	fieldIndex, err = d.fields.GetIndex(name)
	if err != nil {
		return
	}
	relevanceIndex, err = d.relevance.GetIndex(name)
	if err != nil {
		return
	}
	return
}
