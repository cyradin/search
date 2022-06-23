package index

import (
	"testing"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

var _ Storage[uint32, DocSource] = (*testDocStorage)(nil)

type testDocStorage struct {
	one    func(id uint32) (Document[uint32, DocSource], error)
	multi  func(ids ...uint32) ([]Document[uint32, DocSource], error)
	all    func() (<-chan Document[uint32, DocSource], <-chan error)
	insert func(id uint32, doc DocSource) (uint32, error)
	update func(id uint32, doc DocSource) error
	delete func(id uint32) error
}

func (s *testDocStorage) One(id uint32) (Document[uint32, DocSource], error) {
	return s.one(id)
}
func (s *testDocStorage) Multi(ids ...uint32) ([]Document[uint32, DocSource], error) {
	return s.multi(ids...)
}
func (s *testDocStorage) All() (<-chan Document[uint32, DocSource], <-chan error) {
	return s.all()
}
func (s *testDocStorage) Insert(id uint32, doc DocSource) (uint32, error) {
	return s.insert(id, doc)
}
func (s *testDocStorage) Update(id uint32, doc DocSource) error {
	return s.update(id, doc)
}

func (s *testDocStorage) Delete(id uint32) error {
	return s.delete(id)
}

func Test_Documents_Add(t *testing.T) {
	data := []struct {
		name      string
		source    DocSource
		erroneous bool
		expected  uint32
	}{
		{
			name:      "field_value_set_err",
			source:    DocSource{"v": "1"},
			erroneous: true,
			expected:  1,
		},
		{
			name:      "ok",
			source:    DocSource{"v": true},
			erroneous: false,
			expected:  1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			i := New(
				"name",
				schema.New(
					[]schema.Field{schema.NewField("v", schema.TypeBool, true)},
				),
			)

			docs := NewDocuments(t.TempDir())

			err := docs.AddIndex(i)
			require.NoError(t, err)

			guid, err := docs.Add(i, 0, d.source)
			if d.erroneous {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, d.expected, guid)
			}
		})
	}
}
