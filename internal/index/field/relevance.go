package field

import (
	"math"
	"os"
	"sync"

	"github.com/cyradin/search/internal/index/schema"
	jsoniter "github.com/json-iterator/go"
)

const relevanceSuffix = "_relevance"
const relevanceFileExt = ".json"

func SupportsRelevance(f Field) bool {
	return f.Type() == schema.TypeText
}

type relevanceData struct {
	WordCounts   map[string]int            `json:"wordCounts"`   // count of docs containing a word
	DocCounts    map[uint32]map[string]int `json:"docCounts"`    // number of times a word occurs in a document
	DocLengths   map[uint32]int            `json:"docLengths"`   // lengths of docs
	TotalWordCnt int                       `json:"totalWordCnt"` // total word count within the entire index
	AvgDocLen    float64                   `json:"avgDocLen"`    // average doc length within index
}

type Relevance struct {
	mtx  sync.RWMutex
	data relevanceData
}

func NewRelevance() *Relevance {
	return &Relevance{
		data: relevanceData{
			WordCounts: make(map[string]int),
			DocCounts:  make(map[uint32]map[string]int),
			DocLengths: make(map[uint32]int),
		},
	}
}

func (s *Relevance) Add(docID uint32, terms []string) {
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

func (s *Relevance) Delete(docID uint32) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.delete(docID)
}

// IndexWordCount returns number of documents in the index
func (s *Relevance) IndexDocCount() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return len(s.data.DocCounts)
}

// IndexWordCount returns number of documents containing the word
func (s *Relevance) IndexWordCount(word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.WordCounts[word]
}

// DocWordCount returns number of times a word occurs in a document
func (s *Relevance) DocWordCount(docID uint32, word string) int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	m, ok := s.data.DocCounts[docID]
	if !ok {
		return 0
	}

	return m[word]
}

// DocLen returns document length in words
func (s *Relevance) DocLen(docID uint32) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.data.DocLengths[docID]
}

func (s *Relevance) AvgDocLen() float64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.data.AvgDocLen
}

func (i *Relevance) BM25(docID uint32, k1 float64, b float64, word string) float64 {
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

func (i *Relevance) TF(docID uint32, word string) float64 {
	docCnt := i.DocWordCount(docID, word)
	docLen := i.DocLen(docID)

	if docCnt == 0 || docLen == 0 {
		return 0
	}

	return float64(docCnt) / float64(docLen)
}

func (i *Relevance) IDF(docID uint32, word string) float64 {
	wordCnt := float64(i.IndexWordCount(word))
	totalCnt := float64(i.IndexDocCount())

	if wordCnt == 0 || totalCnt == 0 {
		return 0
	}

	return math.Log(totalCnt/wordCnt) + 1
}

func (i *Relevance) TFIDF(docID uint32, word string) float64 {
	return i.TF(docID, word) * i.IDF(docID, word)
}

func (s *Relevance) add(docID uint32, terms []string) {
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

func (s *Relevance) delete(docID uint32) {
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

func (s *Relevance) calcAvgDocLength() {
	docCnt := float64(len(s.data.DocCounts))
	if docCnt == 0 {
		s.data.AvgDocLen = 0
	} else {
		s.data.AvgDocLen = float64(s.data.TotalWordCnt) / docCnt
	}
}

func (s *Relevance) load(src string) error {
	data, err := os.ReadFile(src)
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

func (s *Relevance) dump(src string) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	data, err := jsoniter.Marshal(s.data)
	if err != nil {
		return err
	}

	return os.WriteFile(src, data, filePermissions)
}
