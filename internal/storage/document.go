package storage

type Document[T any] struct {
	ID     string `json:"_id"`
	Source T      `json:"_source"`
}

func newDocument[T any](id string, source T) Document[T] {
	return Document[T]{ID: id, Source: source}
}

func newDocumenFromJSON[T any](data []byte) (Document[T], error) {
	var result Document[T]
	err := json.Unmarshal(data, &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func newDocumentFromJSONMulti[T any](data []byte) ([]Document[T], error) {
	var result []Document[T]
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
