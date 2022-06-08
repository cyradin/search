package index

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/internal/index/schema"
)

var ErrIndexNotFound = fmt.Errorf("index not found")
var ErrIndexAlreadyExists = fmt.Errorf("index already exists")

type Repository struct {
	mtx     sync.Mutex
	storage Storage[string, entity.Index]

	guidGenerate func() string

	dataDir string
	data    map[string]*Documents
}

func NewRepository(ctx context.Context, dataDir string) (*Repository, error) {
	storage, err := NewIndexStorage(dataDir)
	if err != nil {
		return nil, fmt.Errorf("index storage init err: %w", err)
	}

	r := &Repository{
		storage: storage,
		dataDir: dataDir,
		data:    make(map[string]*Documents),
	}

	indexes, err := r.All()
	if err != nil {
		return nil, fmt.Errorf("index list load err: %w", err)
	}

	for _, index := range indexes {
		err := r.initData(ctx, index)
		if err != nil {
			return nil, fmt.Errorf("index data init err: %w", err)
		}
	}

	return r, nil
}

func (r *Repository) Get(name string) (entity.Index, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	doc, err := r.storage.One(name)
	if errors.Is(err, ErrDocNotFound) {
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
		case err, ok := <-errors:
			if ok {
				return nil, fmt.Errorf("storage err: %w", err)
			}
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
	if errors.Is(err, ErrDocAlreadyExists) {
		return ErrIndexAlreadyExists
	}

	err = r.initData(ctx, index)
	if err != nil {
		return err
	}

	return err
}

func (r *Repository) Delete(name string) error {
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

func (r *Repository) Documents(index string) (*Documents, error) {
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
		return fmt.Errorf("source storage dir create err: %w", err)
	}

	sourceStorage, err := NewIndexSourceStorage(r.dataDir, index.Name)
	if err != nil {
		return fmt.Errorf("source storage init err: %w", err)
	}

	data, err := NewDocuments(ctx, index, sourceStorage, fieldPath)
	if err != nil {
		return fmt.Errorf("index data constructor err: %w", err)
	}
	r.data[index.Name] = data

	return nil
}

func (r *Repository) fieldPath(indexName string) string {
	return path.Join(r.dataDir, indexName, "fields")
}
