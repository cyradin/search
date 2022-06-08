package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDoc struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Properties map[string][]string `json:"properties"`
}

var testDoc1 = Document[uint32, testDoc]{
	ID: 1,
	Source: testDoc{
		ID:   "1",
		Name: "foo",
		Properties: map[string][]string{
			"colors": {"red", "blue"},
		},
	},
}
var testDoc2 = Document[uint32, testDoc]{
	ID: 2,
	Source: testDoc{
		ID:   "2",
		Name: "bar",
		Properties: map[string][]string{
			"colors": {"red", "green"},
		},
	},
}

func Test_FileStorage_All(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name      string
		file      string
		expected  []Document[K, T]
		erroneous bool
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
		},
		{
			name:      "ok",
			file:      "../../test/testdata/document/storage_file.json",
			erroneous: false,
			expected:  []Document[uint32, testDoc]{testDoc1, testDoc2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc](d.file)
			if d.erroneous {
				require.Error(t, err)
				return
			}
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
			require.ElementsMatch(t, d.expected, result)
		})
	}
}

func Test_FileStorage_One(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name      string
		file      string
		id        uint32
		expected  Document[K, T]
		erroneous bool
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			id:        0,
			erroneous: true,
		},
		{
			name:      "ok",
			file:      "../../test/testdata/document/storage_file.json",
			id:        1,
			erroneous: false,
			expected:  testDoc1,
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/storage_file.json",
			id:        3,
			erroneous: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc](d.file)
			require.NoError(t, err)

			doc, err := p.One(d.id)
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, d.expected, doc)
		})
	}
}

func Test_FileStorage_Multi(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name      string
		file      string
		ids       []uint32
		expected  []Document[K, T]
		erroneous bool
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
			expected:  []Document[uint32, testDoc]{},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []uint32{1},
			erroneous: false,
			expected:  []Document[uint32, testDoc]{testDoc1},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []uint32{1, 2},
			erroneous: false,
			expected:  []Document[uint32, testDoc]{testDoc1, testDoc2},
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []uint32{3},
			erroneous: false,
			expected:  []Document[uint32, testDoc]{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc](d.file)
			require.NoError(t, err)

			docs, err := p.Multi(d.ids...)
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.ElementsMatch(t, d.expected, docs)
		})
	}
}

func Test_FileStorage_Insert(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name      string
		docs      map[uint32]Document[uint32, T]
		erroneous bool
		expected  uint32
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "ok",
			erroneous: false,
			docs:      make(map[uint32]Document[uint32, testDoc]),
			expected:  1,
		},
		{
			name:      "already_exists",
			erroneous: true,
			docs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc]("")
			require.NoError(t, err)

			p.docs = d.docs

			id, err := p.Insert(0, testDoc{})
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, d.expected, id)
		})
	}
}

func Test_FileStorage_Update(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name      string
		id        uint32
		docs      map[uint32]Document[uint32, T]
		erroneous bool
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "empty_id",
			erroneous: true,
			id:        0,
			docs:      make(map[uint32]Document[uint32, testDoc]),
		},
		{
			name:      "not_exists",
			erroneous: true,
			id:        1,
			docs:      make(map[uint32]Document[uint32, testDoc]),
		},
		{
			name:      "ok",
			erroneous: false,
			id:        1,
			docs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc]("")
			require.NoError(t, err)

			p.docs = d.docs
			err = p.Update(d.id, testDoc{})
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func Test_FileStorage_Delete(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name         string
		id           uint32
		docs         map[uint32]Document[uint32, T]
		erroneous    bool
		expectedDocs map[uint32]Document[uint32, T]
	}

	data := []testData[uint32, testDoc]{
		{
			name:      "empty_id",
			erroneous: true,
			id:        0,
			docs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
			expectedDocs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
		},
		{
			name:      "not_exists",
			erroneous: true,
			id:        2,
			docs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
			expectedDocs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
		},
		{
			name:      "ok",
			erroneous: false,
			id:        1,
			docs: map[uint32]Document[uint32, testDoc]{
				1: {},
			},
			expectedDocs: map[uint32]Document[uint32, testDoc]{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[uint32, testDoc]("")
			require.NoError(t, err)

			p.docs = d.docs
			err = p.Delete(d.id)
			if d.erroneous {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.EqualValues(t, d.docs, d.expectedDocs)
		})
	}
}

func Test_FileStorage_Stop(t *testing.T) {
	type testData[K StorageID, T any] struct {
		name     string
		docs     map[uint32]Document[uint32, T]
		expected string
	}

	data := []testData[uint32, testDoc]{
		{
			name:     "empty",
			docs:     make(map[uint32]Document[uint32, testDoc]),
			expected: "[]",
		},
		{
			name: "not empty",
			docs: map[uint32]Document[uint32, testDoc]{
				1: {
					ID: 1,
					Source: testDoc{
						ID:   "id",
						Name: "name",
						Properties: map[string][]string{
							"color": {"red", "green"},
						},
					},
				},
			},
			expected: `
				[
					{
						"id": 1,
						"source": {
							"id": "id",
							"name": "name",
							"properties": {
								"color": ["red", "green"]
							}
						}
					}
				]`,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "testdir")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			file := filepath.Join(dir, "storage.json")

			p, err := NewFileStorage[uint32, testDoc](file)
			require.NoError(t, err)

			p.docs = d.docs

			ctx := context.Background()
			p.Stop(ctx)

			result, err := os.ReadFile(file)
			require.NoError(t, err)

			require.JSONEq(t, d.expected, string(result))
		})
	}
}
