package document

import (
	jsoniter "github.com/json-iterator/go"

	"github.com/google/uuid"
)

var (
	json        = jsoniter.ConfigCompatibleWithStandardLibrary
	idGenerator = uuid.NewString
)

type Document struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

func NewDocument(id string, source map[string]interface{}) Document {
	return Document{ID: id, Source: source}
}

func NewDocumentFromJSON(data []byte) (Document, error) {
	var result Document
	err := json.Unmarshal(data, &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func NewDocumentsFromJSON(data []byte) ([]Document, error) {
	var result []Document
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
