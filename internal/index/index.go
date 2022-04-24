package index

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
	"github.com/cyradin/search/pkg/finisher"
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

	dataSrc string
	data    map[string]*Data
}

func NewRepository(ctx context.Context, storage storage.Storage[*Index], dataSrc string) (*Repository, error) {
	r := &Repository{
		storage: storage,
		dataSrc: dataSrc,
		data:    make(map[string]*Data),
	}

	indexes, err := r.All()
	if err != nil {
		return nil, err
	}

	for _, index := range indexes {
		err := r.initData(ctx, index)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Repository) Get(name string) (*Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	doc, err := r.storage.One(name)
	nfErr := &storage.ErrNotFound{}
	if errors.As(err, &nfErr) {
		return nil, ErrIndexNotFound
	}

	return doc.Source, nil
}

func (r *Repository) All() ([]*Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var result []*Index

	indexes, errors := r.storage.All()
	for {
		select {
		case doc, ok := <-indexes:
			if !ok {
				return result, nil
			}
			result = append(result, doc.Source)
		case err := <-errors:
			return nil, err
		}
	}
}

func (r *Repository) Add(ctx context.Context, index *Index) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := schema.Validate(index.Schema); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	_, err := r.storage.Insert(index.Name, index)
	nfErr := &storage.ErrAlreadyExists{}
	if errors.As(err, &nfErr) {
		return ErrIndexAlreadyExists
	}

	return r.initData(ctx, index)
}

func (r *Repository) Delete(name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := r.storage.Delete(name); err != nil {
		nfErr := &storage.ErrNotFound{}
		if errors.As(err, &nfErr) {
			return nil
		}

		return err
	}

	return nil
}

func (r *Repository) initData(ctx context.Context, index *Index) error {
	storage, err := storage.NewFile[DocSource](path.Join(r.dataSrc, index.Name+".json"))
	if err != nil {
		return err
	}
	finisher.Add(storage)
	data, err := NewData(ctx, index, storage)

	if err != nil {
		return err
	}
	r.data[index.Name] = data

	return nil
}
