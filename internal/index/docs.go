package index

type (
	idGetter func(uid string) uint32
	idSetter func(uid string) uint32

	sourceStorage interface {
		Insert(guid string, doc DocSource) error
		All() (<-chan DocSource, <-chan error)
	}
)

func (i *Index) Add(guid string, source map[string]interface{}) (string, error) {
	if err := validateDoc(i.schema, source); err != nil {
		return guid, err
	}

	if guid == "" {
		guid = i.guidGenerate()
	}

	err := i.sourceStorage.Insert(guid, source)
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
