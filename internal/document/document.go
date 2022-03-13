package document

import (
	"encoding/json"
	"fmt"
)

type jsonDocs []json.RawMessage

type Document struct {
	ID     string
	Fields map[string]interface{}
}

func NewDocument(id string, fields map[string]interface{}) Document {
	return Document{ID: id, Fields: fields}
}

func NewDocumentFromJSON(idField string, data []byte) (Document, error) {
	var result Document

	doc := make(map[string]interface{})
	err := json.Unmarshal(data, &doc)
	if err != nil {
		return result, err
	}

	id, ok := doc[idField].(string)
	if !ok {
		return result, fmt.Errorf("document id not found")
	}
	result.ID = id
	result.Fields = doc

	return result, nil
}

func NewDocumentsFromJSON(idField string, data []byte) ([]Document, error) {
	var rawDocs jsonDocs
	err := json.Unmarshal(data, &rawDocs)
	if err != nil {
		return nil, err
	}

	result := make([]Document, len(rawDocs))
	for i, rawDoc := range rawDocs {
		doc, err := NewDocumentFromJSON(idField, rawDoc)
		if err != nil {
			return nil, err
		}
		result[i] = doc
	}

	return result, nil
}
