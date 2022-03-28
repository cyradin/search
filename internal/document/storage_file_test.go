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
			file:      "../../test/testdata/document/provider_file.json",
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, err := NewFileStorage(d.file, "id")
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
			file:      "../../test/testdata/document/provider_file.json",
			id:        "1",
			erroneous: false,
			expected:  testDoc1,
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/provider_file.json",
			id:        "3",
			erroneous: true,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, _ := NewFileStorage(d.file, "id")

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
			file:      "../../test/testdata/document/provider_file.json",
			ids:       []string{"1"},
			erroneous: false,
			expected:  []Document{testDoc1},
		},
		{
			name:      "one",
			file:      "../../test/testdata/document/provider_file.json",
			ids:       []string{"1", "2"},
			erroneous: false,
			expected:  []Document{testDoc1, testDoc2},
		},
		{
			name:      "not_found",
			file:      "../../test/testdata/document/provider_file.json",
			ids:       []string{"3"},
			erroneous: false,
			expected:  []Document{},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			p, _ := NewFileStorage(d.file, "id")

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
