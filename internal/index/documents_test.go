package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/index/entity"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

var _ Storage[uint32, entity.DocSource] = (*testDocStorage)(nil)

type testDocStorage struct {
	one    func(id uint32) (Document[uint32, entity.DocSource], error)
	multi  func(ids ...uint32) ([]Document[uint32, entity.DocSource], error)
	all    func() (<-chan Document[uint32, entity.DocSource], <-chan error)
	insert func(id uint32, doc entity.DocSource) (uint32, error)
	update func(id uint32, doc entity.DocSource) error
	delete func(id uint32) error
}

func (s *testDocStorage) One(id uint32) (Document[uint32, entity.DocSource], error) {
	return s.one(id)
}
func (s *testDocStorage) Multi(ids ...uint32) ([]Document[uint32, entity.DocSource], error) {
	return s.multi(ids...)
}
func (s *testDocStorage) All() (<-chan Document[uint32, entity.DocSource], <-chan error) {
	return s.all()
}
func (s *testDocStorage) Insert(id uint32, doc entity.DocSource) (uint32, error) {
	return s.insert(id, doc)
}
func (s *testDocStorage) Update(id uint32, doc entity.DocSource) error {
	return s.update(id, doc)
}

func (s *testDocStorage) Delete(id uint32) error {
	return s.delete(id)
}

func Test_Documents_Add(t *testing.T) {
	data := []struct {
		name         string
		sourceInsert func(id uint32, doc entity.DocSource) (uint32, error)
		source       entity.DocSource
		erroneous    bool
		expected     uint32
	}{
		{
			name:   "source_insert_err",
			source: entity.DocSource{"v": true},
			sourceInsert: func(id uint32, doc entity.DocSource) (uint32, error) {
				return 0, fmt.Errorf("err")
			},
			erroneous: true,
			expected:  1,
		},
		{
			name:   "field_value_set_err",
			source: entity.DocSource{"v": "1"},
			sourceInsert: func(id uint32, doc entity.DocSource) (uint32, error) {
				return 1, nil
			},
			erroneous: true,
			expected:  1,
		},
		{
			name:   "ok",
			source: entity.DocSource{"v": true},
			sourceInsert: func(id uint32, doc entity.DocSource) (uint32, error) {
				return 1, nil
			},
			erroneous: false,
			expected:  1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			docs, err := NewDocuments(
				ctx,
				entity.NewIndex(
					"name",
					schema.New(
						[]schema.Field{schema.NewField("v", field.TypeBool, true)},
					),
				),
				&testDocStorage{
					insert: d.sourceInsert,
				},
				"",
			)
			require.NoError(t, err)

			guid, err := docs.Add(0, d.source)
			if d.erroneous {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, d.expected, guid)
			}
		})
	}
}
