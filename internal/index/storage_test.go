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

var testDoc1 = Document[testDoc]{
	ID: "1",
	Source: testDoc{
		ID:   "1",
		Name: "foo",
		Properties: map[string][]string{
			"colors": {"red", "blue"},
		},
	},
}
var testDoc2 = Document[testDoc]{
	ID: "2",
	Source: testDoc{
		ID:   "2",
		Name: "bar",
		Properties: map[string][]string{
			"colors": {"red", "green"},
		},
	},
}

func Test_File_All(t *testing.T) {
	type testData[T any] struct {
		name      string
		file      string
		expected  []Document[T]
		erroneous bool
	}

	data := []testData[testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
		},
		{
			name:      "ok",
			file:      "../../test/testdata/document/storage_file.json",
			erroneous: false,
			expected:  []Document[testDoc]{testDoc1, testDoc2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc](d.file)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)

			docs, errors := p.All()

			var result []Document[testDoc]
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

			require.Nil(t, err)
			require.ElementsMatch(t, d.expected, result)
		})
	}
}

func Test_File_One(t *testing.T) {
	type testData[T any] struct {
		name      string
		file      string
		id        string
		expected  Document[T]
		erroneous bool
	}

	data := []testData[testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			id:        "",
			erroneous: true,
		},
		{
			name:      "ok",
			file:      "../../test/testdata/document/storage_file.json",
			id:        "1",
			erroneous: false,
			expected:  testDoc1,
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/storage_file.json",
			id:        "3",
			erroneous: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc](d.file)
			require.Nil(t, err)

			doc, err := p.One(d.id)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, doc)
		})
	}
}

func Test_File_Multi(t *testing.T) {
	type testData[T any] struct {
		name      string
		file      string
		ids       []string
		expected  []Document[T]
		erroneous bool
	}

	data := []testData[testDoc]{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
			expected:  []Document[testDoc]{},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"1"},
			erroneous: false,
			expected:  []Document[testDoc]{testDoc1},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"1", "2"},
			erroneous: false,
			expected:  []Document[testDoc]{testDoc1, testDoc2},
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"3"},
			erroneous: false,
			expected:  []Document[testDoc]{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc](d.file)
			require.Nil(t, err)

			docs, err := p.Multi(d.ids...)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.ElementsMatch(t, d.expected, docs)
		})
	}
}

func Test_File_Insert(t *testing.T) {
	type testData[T any] struct {
		name       string
		id         string
		docs       map[string]Document[T]
		erroneous  bool
		expectedId string
	}

	data := []testData[testDoc]{
		{
			name:      "ok",
			erroneous: false,
			id:        "id",
			docs:      make(map[string]Document[testDoc]),
		},
		{
			name:      "already_exists",
			erroneous: true,
			docs: map[string]Document[testDoc]{
				"id": {},
			},
			id: "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc]("")
			p.idGenerator = func() string {
				return d.id
			}
			require.Nil(t, err)

			p.docs = d.docs

			id, err := p.Insert(d.id, testDoc{})
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.id, id)
		})
	}
}

func Test_File_Update(t *testing.T) {
	type testData[T any] struct {
		name      string
		id        string
		docs      map[string]Document[T]
		erroneous bool
	}

	data := []testData[testDoc]{
		{
			name:      "empty_id",
			erroneous: true,
			id:        "",
			docs:      make(map[string]Document[testDoc]),
		},
		{
			name:      "not_exists",
			erroneous: true,
			id:        "id",
			docs:      make(map[string]Document[testDoc]),
		},
		{
			name:      "ok",
			erroneous: false,
			id:        "id",
			docs: map[string]Document[testDoc]{
				"id": {},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc]("")
			require.Nil(t, err)

			p.docs = d.docs
			err = p.Update(d.id, testDoc{})
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
		})
	}
}

func Test_File_Delete(t *testing.T) {
	type testData[T any] struct {
		name         string
		id           string
		docs         map[string]Document[T]
		erroneous    bool
		expectedDocs map[string]Document[T]
	}

	data := []testData[testDoc]{
		{
			name:      "empty_id",
			erroneous: true,
			id:        "",
			docs: map[string]Document[testDoc]{
				"id": {},
			},
			expectedDocs: map[string]Document[testDoc]{
				"id": {},
			},
		},
		{
			name:      "not_exists",
			erroneous: true,
			id:        "id2",
			docs: map[string]Document[testDoc]{
				"id": {},
			},
			expectedDocs: map[string]Document[testDoc]{
				"id": {},
			},
		},
		{
			name:      "ok",
			erroneous: false,
			id:        "id",
			docs: map[string]Document[testDoc]{
				"id": {},
			},
			expectedDocs: map[string]Document[testDoc]{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage[testDoc]("")
			require.Nil(t, err)

			p.docs = d.docs
			err = p.Delete(d.id)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.EqualValues(t, d.docs, d.expectedDocs)
		})
	}
}

func Test_File_Stop(t *testing.T) {
	type testData[T any] struct {
		name     string
		docs     map[string]Document[T]
		expected string
	}

	data := []testData[testDoc]{
		{
			name:     "empty",
			docs:     make(map[string]Document[testDoc]),
			expected: "[]",
		},
		{
			name: "not empty",
			docs: map[string]Document[testDoc]{
				"id": {
					ID: "id",
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
						"_id": "id",
						"_source": {
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
			require.Nil(t, err)
			defer os.RemoveAll(dir)

			file := filepath.Join(dir, "storage.json")

			p, err := NewFileStorage[testDoc](file)
			require.Nil(t, err)

			p.docs = d.docs

			ctx := context.Background()
			p.Stop(ctx)

			result, err := os.ReadFile(file)
			require.Nil(t, err)

			require.JSONEq(t, d.expected, string(result))
		})
	}
}
