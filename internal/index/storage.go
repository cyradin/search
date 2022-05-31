package index

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/pkg/finisher"
	"github.com/google/uuid"

	jsoniter "github.com/json-iterator/go"
)

const dirPermissions = 0755
const filePermissions = 0644

var (
	ErrDocNotFound      = fmt.Errorf("document not found")
	ErrDocAlreadyExists = fmt.Errorf("document with the same id already exists")
	ErrEmptyDocId       = fmt.Errorf("doc id must be defined")
)

type Document[T any] struct {
	ID     string `json:"_id"`
	Source T      `json:"_source"`
}

func newDocument[T any](id string, source T) Document[T] {
	return Document[T]{ID: id, Source: source}
}

func newDocumenFromJSON[T any](data []byte) (Document[T], error) {
	var result Document[T]
	err := jsoniter.Unmarshal(data, &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func newDocumentFromJSONMulti[T any](data []byte) ([]Document[T], error) {
	var result []Document[T]
	err := jsoniter.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type Storage[T any] interface {
	One(id string) (Document[T], error)
	Multi(ids ...string) ([]Document[T], error)
	All() (<-chan Document[T], <-chan error)

	Insert(id string, doc T) (string, error)
	Update(id string, doc T) error
	Delete(id string) error
}

var _ Storage[bool] = (*FileStorage[bool])(nil)

type FileStorage[T any] struct {
	src         string
	idGenerator func() string

	docsMtx sync.RWMutex
	docs    map[string]Document[T]
}

func NewFileStorage[T any](src string) (*FileStorage[T], error) {
	dir := path.Dir(src)
	err := os.MkdirAll(dir, dirPermissions)
	if err != nil {
		return nil, err
	}

	s := &FileStorage[T]{
		src:         src,
		idGenerator: uuid.NewString,
		docs:        make(map[string]Document[T]),
	}

	if err := s.read(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *FileStorage[T]) All() (<-chan Document[T], <-chan error) {
	ch := make(chan Document[T])
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

func (s *FileStorage[T]) One(id string) (Document[T], error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (s *FileStorage[T]) Multi(ids ...string) ([]Document[T], error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	result := make([]Document[T], 0, len(ids))

	for _, id := range ids {
		if doc, ok := s.docs[id]; ok {
			result = append(result, doc)
		}
	}

	return result, nil
}

func (s *FileStorage[T]) Insert(id string, doc T) (string, error) {
	if id == "" {
		id = s.idGenerator()
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; ok {
		return "", ErrDocAlreadyExists
	}
	s.docs[id] = newDocument(id, doc)

	return id, nil
}

func (s *FileStorage[T]) Update(id string, doc T) error {
	if id == "" {
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

func (s *FileStorage[T]) Delete(id string) error {
	if id == "" {
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

func (s *FileStorage[T]) read() error {
	data, err := os.ReadFile(s.src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	docs, err := newDocumentFromJSONMulti[T](data)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		s.docs[doc.ID] = doc
	}

	return nil
}

func (s *FileStorage[T]) Stop(ctx context.Context) error {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	docs := make([]Document[T], 0, len(s.docs))
	for _, doc := range s.docs {
		docs = append(docs, doc)
	}

	data, err := jsoniter.Marshal(docs)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}

func NewIndexStorage(src string) (*FileStorage[entity.Index], error) {
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, err
	}

	path := path.Join(src, "indexes.json")
	storage, err := NewFileStorage[entity.Index](path)
	if err != nil {
		return nil, err
	}
	finisher.Add(storage)

	return storage, nil
}

func NewIndexSourceStorage(src string, name string) (*FileStorage[entity.DocSource], error) {
	src = path.Join(src, name)
	if err := os.MkdirAll(src, dirPermissions); err != nil {
		return nil, err
	}

	storage, err := NewFileStorage[entity.DocSource](path.Join(src, "source.json"))
	if err != nil {
		return nil, err
	}
	finisher.Add(storage)
	return storage, nil
}
