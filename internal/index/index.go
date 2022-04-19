package index

import (
	"context"
	"errors"
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
	mtx     sync.Mutex
	storage storage.Storage[*Index]

	guidGenerate func() string
}

func NewRepository(ctx context.Context, storage storage.Storage[*Index]) *Repository {
	return &Repository{
		storage: storage,
	}
}

func (r *Repository) Get(name string) (*Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	index, err := r.storage.One(name)
	nfErr := &storage.ErrNotFound{}
	if errors.As(err, &nfErr) {
		return nil, ErrIndexNotFound
	}

	return index, nil
}

func (r *Repository) All() ([]*Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var result []*Index

	indexes, errors := r.storage.All()
	for {
		select {
		case index := <-indexes:
			result = append(result, index)
		case err := <-errors:
			return nil, err
		default:
			return result, nil
		}
	}
}

func (r *Repository) Add(index *Index) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := schema.Validate(index.Schema); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	err := r.storage.Insert(index.Name, index)
	nfErr := &storage.ErrAlreadyExists{}
	if !errors.As(err, &nfErr) {
		return ErrIndexAlreadyExists
	}

	return nil
}
