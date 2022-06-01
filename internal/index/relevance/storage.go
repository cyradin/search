package relevance

import "sync"

type Storage struct {
	mtx sync.Mutex

	wordCounts map[string]int            // count of docs containing a word
	docCounts  map[uint32]map[string]int // number of times a word occurs in a document
	docLengths map[uint32]int            // lengths of docs

	totalWordCnt int     // total word count within the entire index
	avgDocLen    float64 // average doc length within index
}

func NewStorage() *Storage {
	return &Storage{
		wordCounts: make(map[string]int),
		docCounts:  make(map[uint32]map[string]int),
		docLengths: make(map[uint32]int),
	}
}

func (s *Storage) Add(docID uint32, terms []string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if len(terms) == 0 {
		return
	}

	if _, ok := s.docCounts[docID]; ok {
		s.delete(docID)
	}
	s.add(docID, terms)
}

func (s *Storage) Delete(docID uint32) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.delete(docID)
}

// IndexWordCount returns number of documents in the index
func (s *Storage) IndexDocCount() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return len(s.docCounts)
}

// IndexWordCount returns number of documents containing the word
func (s *Storage) IndexWordCount(word string) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.wordCounts[word]
}

// DocWordCount returns number of times a word occurs in a document
func (s *Storage) DocWordCount(docID uint32, word string) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	m, ok := s.docCounts[docID]
	if !ok {
		return 0
	}

	return m[word]
}

// DocLen returns document length in words
func (s *Storage) DocLen(docID uint32) int {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.docLengths[docID]
}

func (s *Storage) add(docID uint32, terms []string) {
	if _, ok := s.docCounts[docID]; ok {
		s.Delete(docID)
	}

	counts := make(map[string]int)
	for _, term := range terms {
		counts[term]++
	}

	oldDocCnts := s.docCounts[docID]
	for term, cnt := range oldDocCnts {
		s.wordCounts[term]--
		s.totalWordCnt -= cnt
		if s.wordCounts[term] <= 0 {
			delete(s.wordCounts, term)
		}
	}

	s.docCounts[docID] = make(map[string]int)
	s.docLengths[docID] = len(terms)
	for term, cnt := range counts {
		s.docCounts[docID][term] = cnt
		s.wordCounts[term]++
		s.totalWordCnt += cnt
	}

}

func (s *Storage) delete(docID uint32) {
	m, ok := s.docCounts[docID]
	if !ok {
		return
	}

	for term, cnt := range m {
		s.wordCounts[term]--
		if s.wordCounts[term] <= 0 {
			delete(s.wordCounts, term)
		}
		s.totalWordCnt -= cnt
	}
	delete(s.docCounts, docID)
	delete(s.docLengths, docID)
}
