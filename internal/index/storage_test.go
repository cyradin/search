package index

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDoc struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Properties map[string][]string `json:"properties"`
}

var storageTestData = `
{
    "1": {
        "id": 1,
        "source": {
            "id": "1",
            "name": "foo",
            "properties": {
                "colors": [
                    "red",
                    "blue"
                ]
            }
        }
    },
    "2": {
        "id": 2,
        "source": {
            "id": "2",
            "name": "bar",
            "properties": {
                "colors": [
                    "red",
                    "green"
                ]
            }
        }
    }
}
`

func newTestDoc1() Document[uint32, testDoc] {
	return Document[uint32, testDoc]{
		ID: 1,
		Source: testDoc{
			ID:   "1",
			Name: "foo",
			Properties: map[string][]string{
				"colors": {"red", "blue"},
			},
		},
	}
}

func newTestDoc2() Document[uint32, testDoc] {
	return Document[uint32, testDoc]{
		ID: 2,
		Source: testDoc{
			ID:   "2",
			Name: "bar",
			Properties: map[string][]string{
				"colors": {"red", "green"},
			},
		},
	}
}

func Test_FileStorage(t *testing.T) {
	initStorage := func(t *testing.T) *FileStorage[uint32, testDoc] {
		f := path.Join(t.TempDir(), "/file.json")
		err := os.WriteFile(f, []byte(storageTestData), 0755)
		require.NoError(t, err)
		p, err := NewFileStorage[uint32, testDoc](f)
		require.NoError(t, err)
		return p
	}

	t.Run("All", func(t *testing.T) {
		t.Run("must return empty list if storage file is empty", func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc]("")
			require.NoError(t, err)
			docs, errors := p.All()

			var result []Document[uint32, testDoc]
			a := func() {
				for {
					select {
					case err = <-errors:
						return
					case doc := <-docs:
						result = append(result, doc)
					}
				}
			}
			a()

			require.NoError(t, err)
			require.Len(t, result, 0)
		})
		t.Run("must return not empty list if storage file is not empty", func(t *testing.T) {
			p := initStorage(t)
			docs, errors := p.All()

			var err error
			var result []Document[uint32, testDoc]
			a := func() {
				for {
					select {
					case err = <-errors:
						return
					case doc := <-docs:
						result = append(result, doc)
					}
				}
			}
			a()

			require.NoError(t, err)
			require.ElementsMatch(t, []Document[uint32, testDoc]{newTestDoc1(), newTestDoc2()}, result)
		})
	})

	t.Run("One", func(t *testing.T) {
		t.Run("must return doc from storage if it exists", func(t *testing.T) {
			p := initStorage(t)

			doc, err := p.One(1)
			require.NoError(t, err)
			require.Equal(t, newTestDoc1(), doc)
		})
		t.Run("must return err if doc is not found", func(t *testing.T) {
			p := initStorage(t)

			_, err := p.One(3)
			require.Error(t, err)
		})
	})

	t.Run("Multi", func(t *testing.T) {
		t.Run("must return all found documents", func(t *testing.T) {
			p := initStorage(t)

			docs, err := p.Multi(1, 2, 3)
			require.NoError(t, err)
			require.ElementsMatch(t, []Document[uint32, testDoc]{newTestDoc1(), newTestDoc2()}, docs)
		})
	})

	t.Run("Insert", func(t *testing.T) {
		t.Run("must add document to storage", func(t *testing.T) {
			p := initStorage(t)

			doc := testDoc{ID: "3", Name: "name"}
			id, err := p.Insert(3, doc)
			require.NoError(t, err)
			require.EqualValues(t, 3, id)

			added, err := p.One(3)
			require.NoError(t, err)
			require.Equal(t, doc, added.Source)
		})

		t.Run("must return error if doc already exists", func(t *testing.T) {
			p := initStorage(t)

			_, err := p.Insert(1, testDoc{})
			require.Error(t, err)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("must return err if id is empty", func(t *testing.T) {
			p := initStorage(t)

			err := p.Update(0, testDoc{})
			require.Error(t, err)
		})

		t.Run("must return error if doc not exists", func(t *testing.T) {
			p := initStorage(t)

			err := p.Update(3, testDoc{})
			require.Error(t, err)
		})

		t.Run("must update doc if it exists", func(t *testing.T) {
			p := initStorage(t)

			doc := newTestDoc2()
			doc.Source.Name = "new name"

			err := p.Update(2, doc.Source)
			require.NoError(t, err)

			updated, err := p.One(2)
			require.NoError(t, err)
			require.Equal(t, doc, updated)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("must return err if id is empty", func(t *testing.T) {
			p := initStorage(t)

			err := p.Delete(0)
			require.Error(t, err)
		})
		t.Run("must return error if doc not exists", func(t *testing.T) {
			p := initStorage(t)

			err := p.Delete(3)
			require.Error(t, err)

			doc1, err := p.One(1)
			require.NoError(t, err)
			require.Equal(t, newTestDoc1(), doc1)
			doc2, err := p.One(2)
			require.NoError(t, err)
			require.Equal(t, newTestDoc2(), doc2)
		})
		t.Run("must delete doc if it exists", func(t *testing.T) {
			p := initStorage(t)

			err := p.Delete(2)
			require.NoError(t, err)

			doc1, err := p.One(1)
			require.NoError(t, err)
			require.Equal(t, newTestDoc1(), doc1)
			_, err = p.One(2)
			require.Error(t, err)
		})
	})

	t.Run("Stop", func(t *testing.T) {
		t.Run("must write empty json if storage is empty", func(t *testing.T) {
			f := path.Join(t.TempDir(), "/file.json")
			p, err := NewFileStorage[uint32, testDoc](f)
			require.NoError(t, err)

			err = p.Stop(context.Background())
			require.NoError(t, err)

			result, err := os.ReadFile(f)
			require.NoError(t, err)

			require.JSONEq(t, `{}`, string(result))
		})
		t.Run("must write docs if storage is not empty", func(t *testing.T) {
			p := initStorage(t)

			err := p.Stop(context.Background())
			require.NoError(t, err)

			result, err := os.ReadFile(p.src)
			require.NoError(t, err)

			require.JSONEq(t, storageTestData, string(result))
		})
	})
}
