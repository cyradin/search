package index

import (
	"fmt"
	"os"
	"path"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/query"
	"github.com/cyradin/search/internal/index/relevance"
	"github.com/cyradin/search/internal/index/schema"
)

type (
	idGetter func(uid string) uint32
	idSetter func(uid string) uint32

	sourceStorage interface {
		Insert(guid string, doc entity.DocSource) error
		All() (<-chan entity.DocSource, <-chan error)
	}
)

type Documents struct {
	index            entity.Index
	fieldStorage     *field.Storage
	sourceStorage    Storage[uint32, entity.DocSource]
	relevanceStorage *relevance.Storage
	relevanceCalc    relevance.Calculator
}

func NewDocuments(i entity.Index, sourceStorage Storage[uint32, entity.DocSource], dataPath string) (*Documents, error) {
	fieldPath := path.Join(dataPath, "fields")
	if err := os.MkdirAll(fieldPath, dirPermissions); err != nil {
		return nil, fmt.Errorf("field storage dir create err: %w", err)
	}
	fieldStorage, err := field.NewStorage(fieldPath, i.Schema)
	if err != nil {
		return nil, fmt.Errorf("field storage init err: %w", err)
	}

	result := &Documents{
		index:         i,
		fieldStorage:  fieldStorage,
		sourceStorage: sourceStorage,
	}

	return result, nil
}

func (d *Documents) Add(id uint32, source entity.DocSource) (uint32, error) {
	if err := schema.ValidateDoc(d.index.Schema, source); err != nil {
		return 0, fmt.Errorf("source validation err: %w", err)
	}

	id, err := d.sourceStorage.Insert(id, source)
	if err != nil {
		return id, fmt.Errorf("source insert err: %w", err)
	}

	d.fieldStorage.Add(id, source)

	return id, nil
}

func (d *Documents) Get(id uint32) (entity.DocSource, error) {
	doc, err := d.sourceStorage.One(id)
	if err != nil {
		return nil, fmt.Errorf("source get err: %w", err)
	}

	return doc.Source, err
}

func (d *Documents) Search(q entity.Search) (entity.SearchResult, error) {
	hits, err := query.Exec(q.Query, d.fieldStorage.Fields())
	if err != nil {
		return entity.SearchResult{}, err
	}

	fmt.Println(hits) // @todo make search result

	return entity.SearchResult{}, nil
}
