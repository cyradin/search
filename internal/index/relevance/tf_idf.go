package relevance

import (
	"math"
)

var _ Calculator = (*TFIDF)(nil)

type TFIDF struct {
	storage *Storage
}

func NewTFIDF(s *Storage) *TFIDF {
	return &TFIDF{
		storage: s,
	}
}

func (t *TFIDF) TF(docID uint32, word string) float64 {
	docCnt := t.storage.DocWordCount(docID, word)
	docLen := t.storage.DocLen(docID)

	if docCnt == 0 || docLen == 0 {
		return 0
	}

	return float64(docCnt) / float64(docLen)
}

func (t *TFIDF) IDF(docID uint32, word string) float64 {
	wordCnt := float64(t.storage.IndexWordCount(word))
	totalCnt := float64(t.storage.IndexDocCount())

	if wordCnt == 0 || totalCnt == 0 {
		return 0
	}

	return math.Log(totalCnt/wordCnt) + 1
}

func (t *TFIDF) Calculate(docID uint32, word string) float64 {
	return t.TF(docID, word) * t.IDF(docID, word)
}
