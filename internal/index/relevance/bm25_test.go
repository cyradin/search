package relevance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_BM25_Calculate(t *testing.T) {
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
			expected: 0.42857142857142855,
		},
		{
			name: "one_doc+two_terms",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			docID:    1,
			word:     "bar",
			expected: 0.75,
		},
		{
			name: "two_docs",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
			},
			docID:    2,
			word:     "baz",
			expected: 0.9356865997831276,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {

			s := NewStorage()
			for _, item := range d.data {
				s.Add(item.id, item.words)
			}
			tfidf := NewBM25(s, 2.0, 0.75)

			result := tfidf.Calculate(d.docID, d.word)
			require.Equal(t, d.expected, result)
		})
	}
}
