package relevance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testTFIDFData struct {
	id    uint32
	words []string
}

func Test_TFIDF_Add(t *testing.T) {
	data := []struct {
		name string
		data []testTFIDFData

		indexCnt map[string]int
		docCnt   map[uint32]map[string]int
		docLen   map[uint32]int
	}{
		{
			name:     "empty_data",
			indexCnt: make(map[string]int),
			docCnt:   make(map[uint32]map[string]int),
			docLen:   make(map[uint32]int),
		},
		{
			name: "one",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			indexCnt: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
			},
			docLen: map[uint32]int{
				1: 3,
			},
		},
		{
			name: "two",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
			},
			indexCnt: map[string]int{
				"foo": 2,
				"bar": 2,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
				2: {"foo": 1, "bar": 1, "baz": 2},
			},
			docLen: map[uint32]int{
				1: 3, 2: 4,
			},
		},
		{
			name: "replace",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 1, words: []string{"bar", "baz", "baz", "baz"}},
			},
			indexCnt: map[string]int{
				"bar": 1,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"baz": 3, "bar": 1},
			},
			docLen: map[uint32]int{
				1: 4,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tfidf := NewTFIDF()
			for _, item := range d.data {
				tfidf.Add(item.id, item.words)
			}

			require.EqualValues(t, d.indexCnt, tfidf.indexCounts)
			require.EqualValues(t, d.docCnt, tfidf.docCounts)
			require.EqualValues(t, d.docLen, tfidf.docLen)
		})
	}
}

func Test_TFIDF_Delete(t *testing.T) {
	data := []struct {
		name   string
		data   []testTFIDFData
		delete []uint32

		intCnt map[string]int
		docCnt map[uint32]map[string]int
		docLen map[uint32]int
	}{
		{
			name:   "empty_data",
			delete: []uint32{1},
			intCnt: make(map[string]int),
			docCnt: make(map[uint32]map[string]int),
			docLen: make(map[uint32]int),
		},
		{
			name: "one_not_found",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			delete: []uint32{2},
			intCnt: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
			},
			docLen: map[uint32]int{
				1: 3,
			},
		},
		{
			name: "one_delete",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			delete: []uint32{1},
			intCnt: map[string]int{},
			docCnt: map[uint32]map[string]int{},
			docLen: map[uint32]int{},
		},
		{
			name: "two_delete_one",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
			},
			delete: []uint32{1},
			intCnt: map[string]int{
				"foo": 1,
				"bar": 1,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				2: {"foo": 1, "bar": 1, "baz": 2},
			},
			docLen: map[uint32]int{
				2: 4,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			tfidf := NewTFIDF()
			for _, item := range d.data {
				tfidf.Add(item.id, item.words)
			}

			for _, id := range d.delete {
				tfidf.Delete(id)
			}

			require.EqualValues(t, d.intCnt, tfidf.indexCounts)
			require.EqualValues(t, d.docCnt, tfidf.docCounts)
			require.EqualValues(t, d.docLen, tfidf.docLen)
		})
	}
}

func Test_TFIDF_Calculate(t *testing.T) {
	data := []struct {
		name string
		data []testTFIDFData

		docID    uint32
		word     string
		expected float64
	}{
		{
			name: "empty_data",
		},
		{
			name: "one_doc+one_term",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			docID:    1,
			word:     "foo",
			expected: 0.3333333333333333,
		},
		{
			name: "one_doc+two_terms",
			data: []testTFIDFData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			docID:    1,
			word:     "bar",
			expected: 0.6666666666666666,
		},
		{
			name: "two_docs",
			data: []testTFIDFData{
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
			tfidf := NewTFIDF()
			for _, item := range d.data {
				tfidf.Add(item.id, item.words)
			}

			result := tfidf.Calculate(d.docID, d.word)
			require.Equal(t, d.expected, result)
		})
	}
}
