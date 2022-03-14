package document

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testDoc1 = Document{
	ID: "1",
	Fields: map[string]interface{}{
		"id":   "1",
		"name": "foo",
		"properties": map[string]interface{}{
			"colors": []interface{}{"red", "blue"},
		},
	},
}
var testDoc2 = Document{
	ID: "2",
	Fields: map[string]interface{}{
		"id":   "2",
		"name": "bar",
		"properties": map[string]interface{}{
			"colors": []interface{}{"red", "green"},
		},
	},
}

func Test_FileProvider_All(t *testing.T) {
	data := []struct {
		name      string
		file      string
		expected  []Document
		erroneous bool
	}{
		{
			name:      "invalid_file",
			file:      "invalid",
			erroneous: true,
		},
		{
			name:      "ok",
			file:      "../../test/data/document/provider_file.json",
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p := NewFileProvider(d.file, "id")
			docs, errors := p.All()

			var err error
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

			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, result)
		})
	}
}

func Test_FileProvider_One(t *testing.T) {
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
			file:      "../../test/data/document/provider_file.json",
			id:        "1",
			erroneous: false,
			expected:  testDoc1,
		},
		{
			name:      "not_found",
			file:      "../../test/data/document/provider_file.json",
			id:        "3",
			erroneous: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p := NewFileProvider(d.file, "id")

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

func Test_FileProvider_Multi(t *testing.T) {
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
			erroneous: true,
		},
		{
			name:      "one",
			file:      "../../test/data/document/provider_file.json",
			ids:       []string{"1"},
			erroneous: false,
			expected:  []Document{testDoc1},
		},
		{
			name:      "one",
			file:      "../../test/data/document/provider_file.json",
			ids:       []string{"1", "2"},
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
		{
			name:      "not_found",
			file:      "../../test/data/document/provider_file.json",
			ids:       []string{"3"},
			erroneous: false,
			expected:  nil,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p := NewFileProvider(d.file, "id")

			docs, err := p.Multi(d.ids...)
			if d.erroneous {
				require.NotNil(t, err)
				return
			}

			require.Nil(t, err)
			require.Equal(t, d.expected, docs)
		})
	}
}
