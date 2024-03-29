package field

import (
	"bytes"
	"encoding/gob"
	"math"
	"sync"
)

type scoringData struct {
	WordCounts   map[string]int            `json:"wordCounts"`   // count of docs containing a word
	DocCounts    map[uint32]map[string]int `json:"docCounts"`    // number of times a word occurs in a document
	DocLengths   map[uint32]int            `json:"docLengths"`   // lengths of docs
	TotalWordCnt int                       `json:"totalWordCnt"` // total word count within the entire index
	AvgDocLen    float64                   `json:"avgDocLen"`    // average doc length within index
}

type Scoring struct {
	mtx  sync.RWMutex
	data scoringData
}

func NewScoring() *Scoring {
	return &Scoring{
		data: scoringData{
			WordCounts: make(map[string]int),
			DocCounts:  make(map[uint32]map[string]int),
			DocLengths: make(map[uint32]int),
		},
	}
}

func (s *Scoring) Add(docID uint32, terms []string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if len(terms) == 0 {
		return
	}

	if _, ok := s.data.DocCounts[docID]; ok {
		s.delete(docID)
	}
	s.add(docID, terms)
}

func (s *Scoring) Delete(docID uint32) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.delete(docID)
}

// IndexWordCount returns number of documents in the index
func (s *Scoring) IndexDocCount() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return len(s.data.DocCounts)
}

// IndexWordCount returns number of documents containing the word
func (s *Scoring) IndexWordCount(word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.WordCounts[word]
}

// DocWordCount returns number of times a word occurs in a document
func (s *Scoring) DocWordCount(docID uint32, word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	m, ok := s.data.DocCounts[docID]
	if !ok {
		return 0
	}

	return m[word]
}

// DocLen returns document length in words
func (s *Scoring) DocLen(docID uint32) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.data.DocLengths[docID]
}

func (s *Scoring) AvgDocLen() float64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.AvgDocLen
}

func (i *Scoring) BM25(docID uint32, k1 float64, b float64, word string) float64 {
	tf := i.TF(docID, word)
	if tf == 0 {
		return 0
	}

	idf := i.IDF(docID, word)
	if idf == 0 {
		return 0
	}

	docLen := float64(i.DocLen(docID))
	if docLen == 0 {
		return 0
	}

	return idf * (tf * (k1 + 1)) / (tf + k1*(1-b+b*docLen/i.AvgDocLen()))
}

func (i *Scoring) TF(docID uint32, word string) float64 {
	docCnt := i.DocWordCount(docID, word)
	docLen := i.DocLen(docID)

	if docCnt == 0 || docLen == 0 {
		return 0
	}

	return float64(docCnt) / float64(docLen)
}

func (i *Scoring) IDF(docID uint32, word string) float64 {
	wordCnt := float64(i.IndexWordCount(word))
	totalCnt := float64(i.IndexDocCount())

	if wordCnt == 0 || totalCnt == 0 {
		return 0
	}

	return math.Log(totalCnt/wordCnt) + 1
}

func (i *Scoring) TFIDF(docID uint32, word string) float64 {
	return i.TF(docID, word) * i.IDF(docID, word)
}

func (s *Scoring) add(docID uint32, terms []string) {
	if _, ok := s.data.DocCounts[docID]; ok {
		s.Delete(docID)
	}

	counts := make(map[string]int)
	for _, term := range terms {
		counts[term]++
	}

	oldDocCnts := s.data.DocCounts[docID]
	for term, cnt := range oldDocCnts {
		s.data.WordCounts[term]--
		s.data.TotalWordCnt -= cnt
		if s.data.WordCounts[term] <= 0 {
			delete(s.data.WordCounts, term)
		}
	}

	s.data.DocCounts[docID] = make(map[string]int)
	s.data.DocLengths[docID] = len(terms)
	for term, cnt := range counts {
		s.data.DocCounts[docID][term] = cnt
		s.data.WordCounts[term]++
		s.data.TotalWordCnt += cnt
	}

	s.calcAvgDocLength()
}

func (s *Scoring) delete(docID uint32) {
	m, ok := s.data.DocCounts[docID]
	if !ok {
		return
	}

	for term, cnt := range m {
		s.data.WordCounts[term]--
		if s.data.WordCounts[term] <= 0 {
			delete(s.data.WordCounts, term)
		}
		s.data.TotalWordCnt -= cnt
	}
	delete(s.data.DocCounts, docID)
	delete(s.data.DocLengths, docID)
	s.calcAvgDocLength()
}

func (s *Scoring) calcAvgDocLength() {
	docCnt := float64(len(s.data.DocCounts))
	if docCnt == 0 {
		s.data.AvgDocLen = 0
	} else {
		s.data.AvgDocLen = float64(s.data.TotalWordCnt) / docCnt
	}
}

func (s *Scoring) MarshalBinary() ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(s.data)

	return buf.Bytes(), err
}

func (s *Scoring) UnmarshalBinary(data []byte) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	buf := bytes.NewBuffer(data)

	return gob.NewDecoder(buf).Decode(&s.data)
}
