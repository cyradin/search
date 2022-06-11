package index

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/events"
	"github.com/cyradin/search/internal/index/entity"
	"github.com/google/uuid"

	jsoniter "github.com/json-iterator/go"
)

type StorageID interface {
	~uint32 | ~string
}

const dirPermissions = 0755
const filePermissions = 0644

var (
	ErrDocNotFound      = fmt.Errorf("document not found")
	ErrDocAlreadyExists = fmt.Errorf("document with the same id already exists")
	ErrEmptyDocId       = fmt.Errorf("doc id must be defined")
)

type Document[K StorageID, T any] struct {
	ID     K `json:"id"`
	Source T `json:"source"`
}

func newDocument[K StorageID, T any](id K, source T) Document[K, T] {
	return Document[K, T]{ID: id, Source: source}
}

type Storage[K StorageID, T any] interface {
	One(id K) (Document[K, T], error)
	Multi(ids ...K) ([]Document[K, T], error)
	All() (<-chan Document[K, T], <-chan error)

	Insert(id K, doc T) (K, error)
	Update(id K, doc T) error
	Delete(id K) error
}

var _ Storage[uint32, bool] = (*FileStorage[uint32, bool])(nil)

type FileStorage[K StorageID, T any] struct {
	src string

	docsMtx sync.RWMutex
	docs    map[K]Document[K, T]
}

func NewFileStorage[K StorageID, T any](src string) (*FileStorage[K, T], error) {
	dir := path.Dir(src)
	err := os.MkdirAll(dir, dirPermissions)
	if err != nil {
		return nil, err
	}

	s := &FileStorage[K, T]{
		src:  src,
		docs: make(map[K]Document[K, T]),
	}

	if err := s.read(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *FileStorage[K, T]) All() (<-chan Document[K, T], <-chan error) {
	ch := make(chan Document[K, T])
	errors := make(chan error)

	go func() {
		s.docsMtx.RLock()
		defer s.docsMtx.RUnlock()
		defer close(ch)
		defer close(errors)

		for _, doc := range s.docs {
			ch <- doc
		}
	}()

	return ch, errors
}

func (s *FileStorage[K, T]) One(id K) (Document[K, T], error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (s *FileStorage[K, T]) Multi(ids ...K) ([]Document[K, T], error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	result := make([]Document[K, T], 0, len(ids))

	for _, id := range ids {
		if doc, ok := s.docs[id]; ok {
			result = append(result, doc)
		}
	}

	return result, nil
}

func (s *FileStorage[K, T]) Insert(id K, doc T) (K, error) {
	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	var emptyId K
	if id == emptyId {
		id = s.nextID()
	}

	if _, ok := s.docs[id]; ok {
		return id, ErrDocAlreadyExists
	}
	s.docs[id] = newDocument(id, doc)

	return id, nil
}

func (s *FileStorage[K, T]) Update(id K, doc T) error {
	var emptyId K
	if id == emptyId {
		return ErrEmptyDocId
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; !ok {
		return ErrDocNotFound
	}
	s.docs[id] = newDocument(id, doc)

	return nil
}

func (s *FileStorage[K, T]) Delete(id K) error {
	var emptyId K
	if id == emptyId {
		return ErrEmptyDocId
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; !ok {
		return ErrDocNotFound
	}
	delete(s.docs, id)

	return nil
}

func (s *FileStorage[K, T]) nextID() K {
	var result K
	switch xx := any(result).(type) {
	case string:
		xx = uuid.New().String()
		return any(xx).(K)
	case uint32:
		for _, doc := range s.docs {
			id := any(doc.ID).(uint32)
			if id > xx {
				xx = id
			}
		}
		xx += 1
		return any(xx).(K)
	}

	return result
}

func (s *FileStorage[K, T]) read() error {
	data, err := os.ReadFile(s.src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	err = jsoniter.Unmarshal(data, &s.docs)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileStorage[K, T]) Stop(ctx context.Context) error {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	data, err := jsoniter.Marshal(s.docs)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}

func NewIndexStorage(src string) (*FileStorage[string, entity.Index], error) {
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, err
	}

	path := path.Join(src, "indexes.json")
	storage, err := NewFileStorage[string, entity.Index](path)
	if err != nil {
		return nil, err
	}
	events.Subscribe(events.AppStop{}, func(ctx context.Context, e events.Event) {
		storage.Stop(ctx)
	})

	return storage, nil
}

func NewIndexSourceStorage(src string, name string) (*FileStorage[uint32, entity.DocSource], error) {
	src = path.Join(src, name)
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, err
	}

	storage, err := NewFileStorage[uint32, entity.DocSource](path.Join(src, "source.json"))
	if err != nil {
		return nil, err
	}
	events.Subscribe(events.AppStop{}, func(ctx context.Context, e events.Event) {
		storage.Stop(ctx)
	})

	return storage, nil
}
