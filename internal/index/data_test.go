package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

var _ Storage[entity.DocSource] = (*testDocStorage)(nil)

type testDocStorage struct {
	one    func(id string) (Document[entity.DocSource], error)
	multi  func(ids ...string) ([]Document[entity.DocSource], error)
	all    func() (<-chan Document[entity.DocSource], <-chan error)
	insert func(id string, doc entity.DocSource) (string, error)
	update func(id string, doc entity.DocSource) error
	delete func(id string) error
}

func (s *testDocStorage) One(id string) (Document[entity.DocSource], error) {
	return s.one(id)
}
func (s *testDocStorage) Multi(ids ...string) ([]Document[entity.DocSource], error) {
	return s.multi(ids...)
}
func (s *testDocStorage) All() (<-chan Document[entity.DocSource], <-chan error) {
	return s.all()
}
func (s *testDocStorage) Insert(id string, doc entity.DocSource) (string, error) {
	return s.insert(id, doc)
}
func (s *testDocStorage) Update(id string, doc entity.DocSource) error {
	return s.update(id, doc)
}

func (s *testDocStorage) Delete(id string) error {
	return s.delete(id)
}

func Test_Index_Add(t *testing.T) {
	data := []struct {
		name         string
		guid         string
		generator    func() string
		sourceInsert func(id string, doc entity.DocSource) (string, error)
		source       entity.DocSource
		erroneous    bool
		expected     string
	}{
		{
			name: "empty_id",
			guid: "",
			generator: func() string {
				return "id"
			},
			source: entity.DocSource{"v": true},
			sourceInsert: func(guid string, doc entity.DocSource) (string, error) {
				return "id", nil
			},
			erroneous: false,
			expected:  "id",
		},
		{
			name: "source_insert_err",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: entity.DocSource{"v": true},
			sourceInsert: func(guid string, doc entity.DocSource) (string, error) {
				return "", fmt.Errorf("err")
			},
			erroneous: true,
			expected:  "id",
		},
		{
			name: "field_value_set_err",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: entity.DocSource{"v": "1"},
			sourceInsert: func(guid string, doc entity.DocSource) (string, error) {
				return "id", nil
			},
			erroneous: true,
			expected:  "id",
		},
		{
			name: "ok",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: entity.DocSource{"v": true},
			sourceInsert: func(guid string, doc entity.DocSource) (string, error) {
				return "id", nil
			},
			erroneous: false,
			expected:  "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()

			f, err := field.NewBool(ctx, "")
			require.Nil(t, err)

			index := &Data{
				idSet: func(uid string) uint32 { return 1 },
				fields: map[string]field.Field{
					"v": f,
				},
				sourceStorage: &testDocStorage{
					insert: d.sourceInsert,
				},
			}
			guid, err := index.Add(d.guid, d.source)
			if d.erroneous {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, d.expected, guid)
			}
		})
	}
}
