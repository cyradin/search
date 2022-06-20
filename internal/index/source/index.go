package source

import (
	"fmt"
	"os"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var (
	ErrDocNotFound      = fmt.Errorf("document not found")
	ErrDocAlreadyExists = fmt.Errorf("document with the same id already exists")
	ErrEmptyDocId       = fmt.Errorf("doc id must be defined")
)

type Document struct {
	ID     uint32                 `json:"id"`
	Source map[string]interface{} `json:"source"`
}

func NewDocument(id uint32, source map[string]interface{}) Document {
	return Document{ID: id, Source: source}
}

type Index struct {
	src string

	docsMtx sync.RWMutex
	docs    map[uint32]Document
}

func NewIndex(src string) (*Index, error) {
	s := &Index{
		src:  src,
		docs: make(map[uint32]Document),
	}

	return s, nil
}

func (s *Index) All() (<-chan Document, <-chan error) {
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

func (s *Index) One(id uint32) (Document, error) {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	doc, ok := s.docs[id]
	if !ok {
		return doc, ErrDocNotFound
	}

	return doc, nil
}

func (s *Index) Multi(ids ...uint32) ([]Document, error) {
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

func (s *Index) Insert(id uint32, doc map[string]interface{}) (uint32, error) {
	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if id == 0 {
		id = s.nextID()
	}

	if _, ok := s.docs[id]; ok {
		return id, ErrDocAlreadyExists
	}
	s.docs[id] = NewDocument(id, doc)

	return id, nil
}

func (s *Index) Update(id uint32, doc map[string]interface{}) error {
	if id == 0 {
		return ErrEmptyDocId
	}

	s.docsMtx.Lock()
	defer s.docsMtx.Unlock()

	if _, ok := s.docs[id]; !ok {
		return ErrDocNotFound
	}
	s.docs[id] = NewDocument(id, doc)

	return nil
}

func (s *Index) Delete(id uint32) error {
	if id == 0 {
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

func (s *Index) nextID() uint32 {
	var result uint32
	for _, doc := range s.docs {
		id := doc.ID
		if id > result {
			result = id
		}
	}

	result += 1
	return result
}

func (s *Index) load() error {
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

func (s *Index) dump() error {
	s.docsMtx.RLock()
	defer s.docsMtx.RUnlock()

	data, err := jsoniter.Marshal(s.docs)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}
