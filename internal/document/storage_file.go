package document

import (
	"os"
	"sync"
)

// Storage document storage interface
type Storage interface {
	// All get all documents one-by-one
	All() (<-chan Document, <-chan error)
	// One get one document by ID
	One(id string) (Document, error)
	// Multi get multiple documents by IDs
	Multi(ids ...string) ([]Document, error)
}

var _ Storage = (*FileStorage)(nil)

// FileStorage stores documents in a JSON file.
// It is slow and stupid and is for testing purposes only.
type FileStorage struct {
	src     string
	idField string

	docsMtx sync.RWMutex
	docs    map[string]Document
}

// NewFileStorage returns new instance of FileProvider
func NewFileStorage(src string, idField string) (*FileStorage, error) {
	s := &FileStorage{
		src:     src,
		idField: idField,
		docs:    make(map[string]Document),
	}
	err := s.read()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *FileStorage) All() (<-chan Document, <-chan error) {
	ch := make(chan Document)
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

func (s *FileStorage) One(id string) (Document, error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc, NewErrNotFound(id)
	}

	return doc, nil
}

func (s *FileStorage) Multi(ids ...string) ([]Document, error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	result := make([]Document, 0, len(ids))

	for _, id := range ids {
		if doc, ok := s.docs[id]; ok {
			result = append(result, doc)
		}
	}

	return result, nil
}

func (s *FileStorage) read() error {
	data, err := os.ReadFile(s.src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	docs, err := NewDocumentsFromJSON(s.idField, data)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		s.docs[doc.ID] = doc
	}

	return nil
}
