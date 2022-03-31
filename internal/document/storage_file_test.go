package document

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testDoc1 = Document{
	ID: "1",
	Source: map[string]interface{}{
		"id":   "1",
		"name": "foo",
		"properties": map[string]interface{}{
			"colors": []interface{}{"red", "blue"},
		},
	},
}
var testDoc2 = Document{
	ID: "2",
	Source: map[string]interface{}{
		"id":   "2",
		"name": "bar",
		"properties": map[string]interface{}{
			"colors": []interface{}{"red", "green"},
		},
	},
}

func Test_FileStorage_All(t *testing.T) {
	data := []struct {
		name      string
		file      string
		expected  []Document
		erroneous bool
	}{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
		},
		{
			name:      "ok",
			file:      "../../test/testdata/document/storage_file.json",
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage(context.Background(), d.file)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)

			docs, errors := p.All()

			var result []Document
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
			require.EqualValues(t, d.expected, result)
		})
	}
}

func Test_FileStorage_One(t *testing.T) {
	data := []struct {
		name      string
		file      string
		id        string
		expected  Document
		erroneous bool
	}{
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
			p, _ := NewFileStorage(context.Background(), d.file)

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

func Test_FileStorage_Multi(t *testing.T) {
	data := []struct {
		name      string
		file      string
		ids       []string
		expected  []Document
		erroneous bool
	}{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: false,
			expected:  []Document{},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"1"},
			erroneous: false,
			expected:  []Document{testDoc1},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"1", "2"},
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/storage_file.json",
			ids:       []string{"3"},
			erroneous: false,
			expected:  []Document{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, _ := NewFileStorage(context.Background(), d.file)

			docs, err := p.Multi(d.ids...)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.EqualValues(t, d.expected, docs)
		})
	}
}

func Test_FileStorage_Insert(t *testing.T) {
	data := []struct {
		name      string
		id        string
		expected  string
		docs      map[string]Document
		erroneous bool
	}{
		{
			name:      "empty_id",
			erroneous: true,
			id:        "",
			expected:  "",
			docs:      make(map[string]Document),
		},
		{
			name:      "ok",
			erroneous: false,
			id:        "id",
			expected:  "id",
			docs:      make(map[string]Document),
		},
		{
			name:      "already_exists",
			erroneous: true,
			docs: map[string]Document{
				"id": {},
			},
			id: "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage(context.Background(), "")
			require.Nil(t, err)

			p.docs = d.docs

			id, err := p.Insert(d.id, &Document{})
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, id)
		})
	}
}

func Test_FileStorage_Update(t *testing.T) {
	data := []struct {
		name      string
		id        string
		expected  string
		docs      map[string]Document
		erroneous bool
	}{
		{
			name:      "empty_id",
			erroneous: true,
			id:        "",
			expected:  "id",
			docs:      make(map[string]Document),
		},
		{
			name:      "not_exists",
			erroneous: true,
			id:        "id",
			docs:      make(map[string]Document),
		},
		{
			name:      "ok",
			erroneous: false,
			docs: map[string]Document{
				"id": {},
			},
			id:       "id",
			expected: "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage(context.Background(), "")
			require.Nil(t, err)

			p.docs = d.docs

			id, err := p.Update(d.id, &Document{})
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, id)
		})
	}
}

func Test_FileStorage_dumpOnCancel(t *testing.T) {
	data := []struct {
		name     string
		docs     map[string]Document
		expected string
	}{
		{
			name:     "empty",
			docs:     make(map[string]Document),
			expected: "[]",
		},
		{
			name: "not empty",
			docs: map[string]Document{
				"id": {
					ID: "id",
					Source: map[string]interface{}{
						"text": "qwerty",
					},
				},
			},
			expected: `
				[
					{
						"_id": "id",
						"_source": {
							"text": "qwerty"
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

			ctx, cancel := context.WithCancel(context.Background())

			p, err := NewFileStorage(ctx, file)
			require.Nil(t, err)

			p.docs = d.docs

			cancel()
			time.Sleep(100 * time.Millisecond)

			result, err := os.ReadFile(file)
			require.Nil(t, err)

			require.JSONEq(t, d.expected, string(result))
		})
	}
}
