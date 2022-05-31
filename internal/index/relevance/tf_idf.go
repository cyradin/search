package relevance

import (
	"math"
	"sync"
)

type TFIDF struct {
	mtx         sync.Mutex
	indexCounts map[string]int            // count of docs containing a word
	docCounts   map[uint32]map[string]int // number of times a word occurs in a document
	docLen      map[uint32]int            // lengths of docs
}

func NewTFIDF() *TFIDF {
	return &TFIDF{
		indexCounts: make(map[string]int),
		docCounts:   make(map[uint32]map[string]int),
		docLen:      make(map[uint32]int),
	}
}

func (t *TFIDF) Add(docID uint32, terms []string) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	counts := make(map[string]int)
	for _, term := range terms {
		counts[term]++
	}

	oldDocFreqs := t.docCounts[docID]
	for term := range oldDocFreqs {
		t.indexCounts[term]--
		if t.indexCounts[term] <= 0 {
			delete(t.indexCounts, term)
		}
	}

	t.docCounts[docID] = make(map[string]int)
	t.docLen[docID] = len(terms)
	for term, freq := range counts {
		t.docCounts[docID][term] = freq
		t.indexCounts[term]++
	}
}

func (t *TFIDF) Delete(docID uint32) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	m, ok := t.docCounts[docID]
	if !ok {
		return
	}
	for term := range m {
		t.indexCounts[term]--
		if t.indexCounts[term] <= 0 {
			delete(t.indexCounts, term)
		}
	}
	delete(t.docCounts, docID)
	delete(t.docLen, docID)
}

func (t *TFIDF) Calculate(docID uint32, word string) float64 {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	m, ok := t.docCounts[docID]
	if !ok {
		return 0
	}

	docCnt := float64(m[word])
	docLen := float64(t.docLen[docID])
	indexCnt := float64(t.indexCounts[word])
	indexDocCnt := float64(len(t.docLen)) // total doc count

	if docLen == 0 || docCnt == 0 || indexCnt == 0 || indexDocCnt == 0 {
		return 0
	}

	tf := docCnt / docLen
	idf := math.Log(indexDocCnt/indexCnt) + 1

	return tf * idf
}
