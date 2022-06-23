package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyradin/search/internal/index/schema"
)

var ErrIndexNotFound = fmt.Errorf("index not found")
var ErrIndexAlreadyExists = fmt.Errorf("index already exists")

type Index struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Schema    schema.Schema `json:"schema"`
}

func New(name string, s schema.Schema) Index {
	return Index{
		Name:      name,
		CreatedAt: time.Now(),
		Schema:    s,
	}
}

type Repository struct {
	mtx     sync.Mutex
	storage Storage[string, Index]

	dataDir string

	docs *Documents
}

func NewRepository(dataDir string, docs *Documents) (*Repository, error) {
	storage, err := NewIndexStorage(dataDir)
	if err != nil {
		return nil, fmt.Errorf("index storage init err: %w", err)
	}

	return &Repository{
		storage: storage,
		dataDir: dataDir,
		docs:    docs,
	}, nil
}

func (r *Repository) Init(ctx context.Context) error {
	indexes, err := r.All()
	if err != nil {
		return fmt.Errorf("index list load err: %w", err)
	}

	for _, index := range indexes {
		err := r.docs.AddIndex(index)
		if err != nil {
			return fmt.Errorf("index data init err: %w", err)
		}
	}

	return nil
}

func (r *Repository) Get(name string) (Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	doc, err := r.storage.One(name)
	if errors.Is(err, ErrDocNotFound) {
		return Index{}, ErrIndexNotFound
	}

	return doc.Source, nil
}

func (r *Repository) All() ([]Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var result []Index

	indexes, errors := r.storage.All()
	for {
		select {
		case doc, ok := <-indexes:
			if !ok {
				return result, nil
			}
			result = append(result, doc.Source)
		case err, ok := <-errors:
			if ok {
				return nil, fmt.Errorf("storage err: %w", err)
			}
		}
	}
}

func (r *Repository) Add(ctx context.Context, index Index) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := schema.Validate(index.Schema); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	_, err := r.storage.Insert(index.Name, index)
	if errors.Is(err, ErrDocAlreadyExists) {
		return ErrIndexAlreadyExists
	}

	return err
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := r.storage.Delete(name); err != nil {
		if errors.Is(err, ErrDocNotFound) {
			return nil
		}

		return fmt.Errorf("index delete err: %w", err)
	}

	return nil
}
