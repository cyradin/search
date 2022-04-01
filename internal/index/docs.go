package index

import (
	"github.com/cyradin/search/internal/document"
)

type (
	idGetter func(uid string) uint32
	idSetter func(uid string) uint32

	sourceInserter func(guid string, doc *document.Document) error
)

func (i *Index) Add(guid string, source map[string]interface{}) (string, error) {
	if guid == "" {
		guid = i.guidGenerate()
	}

	doc := document.New(guid, source)
	err := i.sourceInsert(guid, &doc)
	if err != nil {
		return guid, err
	}

	id := i.idSet(guid)

	for name, field := range i.fields {
		if v, ok := source[name]; ok {
			err := field.AddValue(id, v)
			if err != nil {
				return guid, err
			}
		}
	}

	return guid, nil
}
