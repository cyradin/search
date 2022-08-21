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

func New(name string, s schema.Schema) IndexData {
	return IndexData{
		Name:      name,
		CreatedAt: time.Now(),
		Schema:    s,
	}
}

type Repository struct {
	mtx sync.RWMutex

	items   map[string]*Index
	storage *storage.KeyedDictStorage[IndexData]
}

func NewRepository(strg *storage.DictStorage[IndexData]) (*Repository, error) {
	return &Repository{
		items:   make(map[string]*Index),
		storage: strg.WithKey("indexes"),
	}, nil
}

func (r *Repository) Init(ctx context.Context) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	indexes, err := r.storage.AllValues(ctx)
	if err != nil {
		return errs.Errorf("index list load err: %w", err)
	}

	for _, index := range indexes {
		i, err := NewIndex(index)
		if err != nil {
			// @todo mark index as broken and continue
			return errs.Errorf("index data init err: %w", err)
		}

		r.items[index.Name] = i

	}

	return nil
}

func (r *Repository) Get(ctx context.Context, name string) (*Index, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result, ok := r.items[name]
	if !ok {
		return nil, ErrIndexNotFound
	}

	return result, nil
}

func (r *Repository) All(ctx context.Context) ([]*Index, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result := make([]*Index, 0, len(r.items))
	for _, item := range r.items {
		result = append(result, item)
	}

	return result, nil
}

func (r *Repository) Add(ctx context.Context, index IndexData) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.items[index.Name]; ok {
		return ErrIndexAlreadyExists
	}

	if err := validation.Validate(index.Schema); err != nil {
		return errs.Errorf("schema validation failed: %w", err)
	}

	i, err := NewIndex(index)
	if err != nil {
		return errs.Errorf("index init err: %w", err)
	}

	if err := r.storage.Set(ctx, index.Name, index); err != nil {
		return err
	}
	r.items[index.Name] = i

	return nil
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := r.storage.Del(ctx, name); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}

		return errs.Errorf("index delete err: %w", err)
	}
	delete(r.items, name)

	return nil
}