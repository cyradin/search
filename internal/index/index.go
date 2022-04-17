package index

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
)

var ErrIndexNotFound = fmt.Errorf("index not found")
var ErrIndexAlreadyExists = fmt.Errorf("index already exists")

type Index struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Schema    schema.Schema `json:"schema"`
}

func New(ctx context.Context, name string, s schema.Schema) *Index {
	return &Index{
		Name:      name,
		CreatedAt: time.Now(),
		Schema:    s,
	}
}

type Repository struct {
	indexesMtx sync.RWMutex
	indexes    map[string]*Index

	storage storage.Storage[*Index]

	guidGenerate func() string
}

func NewRepository(ctx context.Context, storage storage.Storage[*Index]) *Repository {
	r := &Repository{
		indexes: make(map[string]*Index),
		storage: storage,
	}

	return r
}

func (r *Repository) Get(name string) (*Index, error) {
	r.indexesMtx.RLock()
	defer r.indexesMtx.RUnlock()

	index, ok := r.indexes[name]
	if !ok {
		return nil, ErrIndexNotFound
	}

	return index, nil
}

func (r *Repository) All() ([]*Index, error) {
	r.indexesMtx.RLock()
	defer r.indexesMtx.RUnlock()

	result := make([]*Index, 0, len(r.indexes))
	for _, index := range r.indexes {
		result = append(result, index)
	}

	return result, nil
}

func (r *Repository) Add(index *Index) error {
	r.indexesMtx.Lock()
	defer r.indexesMtx.Unlock()

	if _, ok := r.indexes[index.Name]; ok {
		return ErrIndexAlreadyExists
	}

	if err := schema.Validate(index.Schema); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	r.indexes[index.Name] = index

	return nil
}

func (r *Repository) load(ctx context.Context) error {
	r.indexesMtx.RLock()
	defer r.indexesMtx.RUnlock()

	indexes, errors := r.storage.All()
	for {
		select {
		case indexRaw := <-indexes:
			fmt.Println(indexRaw) // @todo load index
		case err := <-errors:
			return err
		}
	}
}
