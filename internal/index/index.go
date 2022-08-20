package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
	validation "github.com/go-ozzo/ozzo-validation/v4"
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
	mtx sync.Mutex

	docs    *Documents
	storage *storage.KeyedDictStorage[Index]
}

func NewRepository(strg *storage.DictStorage[Index], docs *Documents) (*Repository, error) {
	return &Repository{
		docs:    docs,
		storage: strg.WithKey("indexes"),
	}, nil
}

func (r *Repository) Init(ctx context.Context) error {
	indexes, err := r.All(ctx)
	if err != nil {
		return errs.Errorf("index list load err: %w", err)
	}

	for _, index := range indexes {
		err := r.docs.AddIndex(index)
		if err != nil {
			return errs.Errorf("index data init err: %w", err)
		}
	}

	return nil
}

func (r *Repository) Get(ctx context.Context, name string) (Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	result, err := r.storage.Get(ctx, name)
	if errors.Is(err, storage.ErrNotFound) {
		return result, ErrIndexNotFound
	}

	return result, nil
}

func (r *Repository) All(ctx context.Context) ([]Index, error) {
	result, err := r.storage.AllValues(ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Repository) Add(ctx context.Context, index Index) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := validation.Validate(index.Schema); err != nil {
		return errs.Errorf("schema validation failed: %w", err)
	}

	if err := r.docs.AddIndex(index); err != nil {
		return errs.Errorf("docs index add err: %w", err)
	}

	if err := r.storage.Set(ctx, index.Name, index); err != nil {
		// @todo
		// if errors.Is(err, storage.ErrDocAlreadyExists) {
		// 	return ErrIndexAlreadyExists
		// }
	}

	return nil
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := r.docs.DeleteIndex(name); err != nil {
		return errs.Errorf("docs index delete err: %w", err)
	}

	if err := r.storage.Del(ctx, name); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}

		return errs.Errorf("index delete err: %w", err)
	}

	return nil
}
