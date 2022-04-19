package storage

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/cyradin/search/pkg/ctxt"
	"go.uber.org/zap"
)

const filePermissions = 0644

type Storage[T any] interface {
	One(id string) (T, error)
	Multi(ids ...string) ([]T, error)
	All() (<-chan T, <-chan error)

	Insert(id string, doc T) error
	Update(id string, doc T) error
}

var _ Storage[bool] = (*File[bool])(nil)

type File[T any] struct {
	src string

	docsMtx sync.RWMutex
	docs    map[string]document[T]
}

func NewFile[T any](ctx context.Context, src string) (*File[T], error) {
	s := &File[T]{
		src:  src,
		docs: make(map[string]document[T]),
	}

	err := os.MkdirAll(src, filePermissions)
	if err != nil {
		return nil, err
	}

	err = s.read()
	if err != nil {
		return nil, err
	}

	s.dumpOnCancel(ctx)

	return s, nil
}

func (s *File[T]) All() (<-chan T, <-chan error) {
	ch := make(chan T)
	errors := make(chan error)

	go func() {
		s.docsMtx.RLock()
		defer s.docsMtx.RUnlock()
		defer close(ch)
		defer close(errors)

		for _, doc := range s.docs {
			ch <- doc.Source
		}
	}()

	return ch, errors
}

func (s *File[T]) One(id string) (T, error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc.Source, NewErrNotFound(id)
	}

	return doc.Source, nil
}

func (s *File[T]) Multi(ids ...string) ([]T, error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	result := make([]T, 0, len(ids))

	for _, id := range ids {
		if doc, ok := s.docs[id]; ok {
			result = append(result, doc.Source)
		}
	}

	return result, nil
}

func (s *File[T]) Insert(id string, doc T) error {
	if id == "" {
		return NewErrEmptyId()
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; ok {
		return NewErrAlreadyExists(id)
	}
	s.docs[id] = newDocument(id, doc)

	return nil
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
	ctxt.Wg(ctx).Add(1)
	go func() {
		select {
		case <-ctx.Done():
			defer ctxt.Wg(ctx).Done()
			ctxt.Logger(ctx).Debug("storage.dump.start", ctxt.ExtractFields(ctx)...)
			err := s.dump()
			if err != nil {
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

	docs := make([]document[T], 0, len(s.docs))
	for _, doc := range s.docs {
		docs = append(docs, doc)
	}

	data, err := json.Marshal(docs)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}
