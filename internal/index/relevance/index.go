package relevance

import (
	"os"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

type indexData struct {
	WordCounts   map[string]int            `json:"wordCounts"`   // count of docs containing a word
	DocCounts    map[uint32]map[string]int `json:"docCounts"`    // number of times a word occurs in a document
	DocLengths   map[uint32]int            `json:"docLengths"`   // lengths of docs
	TotalWordCnt int                       `json:"totalWordCnt"` // total word count within the entire index
	AvgDocLen    float64                   `json:"avgDocLen"`    // average doc length within index
}

type Index struct {
	src  string
	mtx  sync.RWMutex
	data indexData
}

func NewIndex(src string) *Index {
	return &Index{
		src: src,
		data: indexData{
			WordCounts: make(map[string]int),
			DocCounts:  make(map[uint32]map[string]int),
			DocLengths: make(map[uint32]int),
		},
	}
}

func (s *Index) Add(docID uint32, terms []string) {
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

func (s *Index) Delete(docID uint32) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.delete(docID)
}

// IndexWordCount returns number of documents in the index
func (s *Index) IndexDocCount() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return len(s.data.DocCounts)
}

// IndexWordCount returns number of documents containing the word
func (s *Index) IndexWordCount(word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.WordCounts[word]
}

// DocWordCount returns number of times a word occurs in a document
func (s *Index) DocWordCount(docID uint32, word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	m, ok := s.data.DocCounts[docID]
	if !ok {
		return 0
	}

	return m[word]
}

// DocLen returns document length in words
func (s *Index) DocLen(docID uint32) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.data.DocLengths[docID]
}

func (s *Index) AvgDocLen() float64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.AvgDocLen
}

func (s *Index) add(docID uint32, terms []string) {
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

func (s *Index) delete(docID uint32) {
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

func (s *Index) calcAvgDocLength() {
	docCnt := float64(len(s.data.DocCounts))
	if docCnt == 0 {
		s.data.AvgDocLen = 0
	} else {
		s.data.AvgDocLen = float64(s.data.TotalWordCnt) / docCnt
	}
}

func (s *Index) load() error {
	data, err := os.ReadFile(s.src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	err = jsoniter.Unmarshal(data, &s.data)
	if err != nil {
		return err
	}

	return nil
}

func (s *Index) dump() error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	data, err := jsoniter.Marshal(s.data)
	if err != nil {
		return err
	}

	return os.WriteFile(s.src, data, filePermissions)
}
