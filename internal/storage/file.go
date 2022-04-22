package storage

import (
	"context"
	"os"
	"sync"

	"github.com/cyradin/search/pkg/ctxt"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

const dirPermissions = 0755
const filePermissions = 0644

type Storage[T any] interface {
	One(id string) (Document[T], error)
	Multi(ids ...string) ([]Document[T], error)
	All() (<-chan Document[T], <-chan error)

	Insert(id string, doc T) (string, error)
	Update(id string, doc T) error
	Delete(id string) error
}

var _ Storage[bool] = (*File[bool])(nil)

type File[T any] struct {
	src         string
	idGenerator func() string

	docsMtx sync.RWMutex
	docs    map[string]Document[T]
}

func NewFile[T any](ctx context.Context, src string) (*File[T], error) {
	s := &File[T]{
		src:         src,
		idGenerator: uuid.NewString,
		docs:        make(map[string]Document[T]),
	}

	if err := s.read(); err != nil {
		return nil, err
	}

	s.dumpOnCancel(ctx)

	return s, nil
}

func (s *File[T]) All() (<-chan Document[T], <-chan error) {
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

func (s *File[T]) One(id string) (Document[T], error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc, NewErrNotFound(id)
	}

	return doc, nil
}

func (s *File[T]) Multi(ids ...string) ([]Document[T], error) {
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

func (s *File[T]) Insert(id string, doc T) (string, error) {
	if id == "" {
		id = s.idGenerator()
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; ok {
		return "", NewErrAlreadyExists(id)
	}
	s.docs[id] = newDocument(id, doc)

	return id, nil
}

func (s *File[T]) Update(id string, doc T) error {
	if id == "" {
		return NewErrEmptyId()
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; !ok {
		return NewErrNotFound(id)
	}
	s.docs[id] = newDocument(id, doc)

	return nil
}

func (s *File[T]) Delete(id string) error {
	if id == "" {
		return NewErrEmptyId()
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; !ok {
		return NewErrNotFound(id)
	}
	delete(s.docs, id)

	return nil
}

func (s *File[T]) read() error {
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

func (s *File[T]) dumpOnCancel(ctx context.Context) {
	wg := ctxt.Wg(ctx)
	wg.Add(1)
	go func() {
		select {
		case <-ctx.Done():
			defer wg.Done()
			ctxt.Logger(ctx).Debug("storage.dump.start", ctxt.ExtractFields(ctx)...)
			if err := s.dump(); err != nil {
				ctxt.Logger(ctx).Error("storage.dump.error", ctxt.ExtractFields(ctx, zap.Error(err))...)
				return
			}
			ctxt.Logger(ctx).Debug("storage.dump.finish", ctxt.ExtractFields(ctx)...)
		}
	}()
}

func (s *File[T]) dump() error {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	docs := make([]Document[T], 0, len(s.docs))
	for _, doc := range s.docs {
		docs = append(docs, doc)
	}

	data, err := json.Marshal(docs)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}
