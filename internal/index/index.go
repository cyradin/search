package index

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
)

var ErrIndexNotFound = fmt.Errorf("index not found")
var ErrIndexAlreadyExists = fmt.Errorf("index already exists")

type storageFactory interface {
	NewIndexStorage() (storage.Storage[entity.Index], error)
	NewIndexSourceStorage(name string) (storage.Storage[entity.DocSource], error)
}

type Repository struct {
	mtx            sync.Mutex
	storage        storage.Storage[entity.Index]
	storageFactory storageFactory

	guidGenerate func() string

	dataSrc string
	data    map[string]*Data
}

func NewRepository(ctx context.Context, storageFactory storageFactory, dataSrc string) (*Repository, error) {
	storage, err := storageFactory.NewIndexStorage()
	if err != nil {
		return nil, err
	}

	r := &Repository{
		storageFactory: storageFactory,
		storage:        storage,
		dataSrc:        dataSrc,
		data:           make(map[string]*Data),
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
	nfErr := &storage.ErrNotFound{}
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
	sourceStorage, err := r.storageFactory.NewIndexSourceStorage(index.Name)
	if err != nil {
		return err
	}

	data, err := NewData(ctx, index, sourceStorage)

	if err != nil {
		return err
	}
	r.data[index.Name] = data

	return nil
}
