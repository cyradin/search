package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/cyradin/search/internal/storage"
	"github.com/stretchr/testify/require"
)

var _ storage.Storage[DocSource] = (*testDocStorage)(nil)

type testDocStorage struct {
	one    func(id string) (DocSource, error)
	multi  func(ids ...string) ([]DocSource, error)
	all    func() (<-chan DocSource, <-chan error)
	insert func(id string, doc DocSource) error
	update func(id string, doc DocSource) error
}

func (s *testDocStorage) One(id string) (DocSource, error) {
	return s.one(id)
}
func (s *testDocStorage) Multi(ids ...string) ([]DocSource, error) {
	return s.multi(ids...)
}
func (s *testDocStorage) All() (<-chan DocSource, <-chan error) {
	return s.all()
}
func (s *testDocStorage) Insert(id string, doc DocSource) error {
	return s.insert(id, doc)
}
func (s *testDocStorage) Update(id string, doc DocSource) error {
	return s.update(id, doc)
}

func Test_Index_Add(t *testing.T) {
	data := []struct {
		name         string
		guid         string
		generator    func() string
		sourceInsert func(id string, doc DocSource) error
		source       DocSource
		erroneous    bool
		expected     string
	}{
		{
			name: "empty_id",
			guid: "",
			generator: func() string {
				return "id"
			},
			source: DocSource{"v": true},
			sourceInsert: func(guid string, doc DocSource) error {
				return nil
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
			source: DocSource{"v": true},
			sourceInsert: func(guid string, doc DocSource) error {
				return fmt.Errorf("err")
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
			source: DocSource{"v": "1"},
			sourceInsert: func(guid string, doc DocSource) error {
				return nil
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
			source: DocSource{"v": true},
			sourceInsert: func(guid string, doc DocSource) error {
				return nil
			},
			erroneous: false,
			expected:  "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()

			index := &Index{
				idSet: func(uid string) uint32 { return 1 },
				fields: map[string]field.Field{
					"v": field.NewBool(ctx),
				},
				guidGenerate: d.generator,
				schema:       schema.Schema{},
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
