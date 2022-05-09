package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/schema"
)

var ErrIndexNotFound = fmt.Errorf("index not found")
var ErrIndexAlreadyExists = fmt.Errorf("index already exists")

type Repository struct {
	mtx           sync.Mutex
	storage       Storage[entity.Index]
	sourceStorage Storage[entity.DocSource]

	guidGenerate func() string

	dataDir string
	data    map[string]*Data
}

func NewRepository(ctx context.Context, dataDir string) (*Repository, error) {
	storage, err := NewIndexStorage(dataDir)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		storage: storage,
		dataDir: dataDir,
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

func (r *Repository) Get(name string) (entity.Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	doc, err := r.storage.One(name)
	nfErr := &ErrNotFound{}
	if errors.As(err, &nfErr) {
		return entity.Index{}, ErrIndexNotFound
	}

	return doc.Source, nil
}

func (r *Repository) All() ([]entity.Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var result []entity.Index

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

func (r *Repository) Add(ctx context.Context, index entity.Index) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := schema.Validate(index.Schema); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	_, err := r.storage.Insert(index.Name, index)
	nfErr := &ErrAlreadyExists{}
	if errors.As(err, &nfErr) {
		return ErrIndexAlreadyExists
	}

	return r.initData(ctx, index)
}

func (r *Repository) Delete(name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if err := r.storage.Delete(name); err != nil {
		nfErr := &ErrNotFound{}
		if errors.As(err, &nfErr) {
			return nil
		}

		return err
	}

	return nil
}

func (r *Repository) Data(index string) (*Data, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	data, ok := r.data[index]
	if !ok {
		return nil, ErrIndexNotFound
	}

	return data, nil
}

func (r *Repository) initData(ctx context.Context, index entity.Index) error {
	fieldPath := r.fieldPath(index.Name)
	if err := os.MkdirAll(fieldPath, dirPermissions); err != nil {
		return err
	}

	sourceStorage, err := NewIndexSourceStorage(r.dataDir, index.Name)
	if err != nil {
		return err
	}

	data, err := NewData(ctx, index, sourceStorage, fieldPath)

	if err != nil {
		return err
	}
	r.data[index.Name] = data

	return nil
}

func (r *Repository) fieldPath(indexName string) string {
	return path.Join(r.dataDir, indexName, "fields")
}
