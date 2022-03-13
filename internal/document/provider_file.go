package document

import (
	"io/ioutil"
	"os"
)

// Provider document provider interface
type Provider interface {
	// All get all documents one-by-one
	All() <-chan Document
	// One get one document by ID
	One(id string) (Document, error)
	// Multi get multiple documents by IDs
	Multi(ids ...string) ([]Document, error)
}

var _ Provider = (*FileProvider)(nil)

// FileProvider returns documents from JSON file.
// It is slow and stupid and is for testing purposes only.
type FileProvider struct {
	src     string
	idField string
}

// NewFileProvider returns new instance of FileProvider
func NewFileProvider(src string, idField string) *FileProvider {
	return &FileProvider{
		src:     src,
		idField: idField,
	}
}

func (p *FileProvider) All() <-chan Document {
	// @todo
	return nil
}

func (p *FileProvider) One(id string) (Document, error) {
	var result Document

	docs, err := p.read()
	if err != nil {
		return result, err
	}

	for _, doc := range docs {
		if doc.ID != id {
			continue
		}

		result = doc
	}

	if result.ID == "" {
		return result, NewErrNotFound(id)
	}

	return result, nil
}

func (p *FileProvider) Multi(ids ...string) ([]Document, error) {
	var result []Document

	docs, err := p.read()
	if err != nil {
		return result, err
	}

	idMap := make(map[string]struct{})
	for _, id := range ids {
		idMap[id] = struct{}{}
	}

	for _, doc := range docs {
		if _, ok := idMap[doc.ID]; !ok {
			continue
		}

		result = append(result, doc)
	}

	return result, nil
}

func (p *FileProvider) read() ([]Document, error) {
	f, err := os.Open(p.src)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return NewDocumentsFromJSON(p.idField, data)
}
