package relevance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testStorageData struct {
	id    uint32
	words []string
}

func Test_TFIDF_Calculate(t *testing.T) {
	data := []struct {
		name string
		data []testStorageData

		docID    uint32
		word     string
		expected float64
	}{
		{
			name: "empty_data",
		},
		{
			name: "one_doc+one_term",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			docID:    1,
			word:     "foo",
			expected: 0.3333333333333333,
		},
		{
			name: "one_doc+two_terms",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			docID:    1,
			word:     "bar",
			expected: 0.6666666666666666,
		},
		{
			name: "two_docs",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
			},
			docID:    2,
			word:     "baz",
			expected: 0.8465735902799727,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {

			s := NewStorage()
			for _, item := range d.data {
				s.Add(item.id, item.words)
			}
			tfidf := NewTFIDF(s)

			result := tfidf.Calculate(d.docID, d.word)
			require.Equal(t, d.expected, result)
		})
	}
}
