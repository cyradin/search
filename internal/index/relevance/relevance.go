package relevance

import "math"

func (i *Index) BM25(docID uint32, k1 float64, b float64, word string) float64 {
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

func (i *Index) TF(docID uint32, word string) float64 {
	docCnt := i.DocWordCount(docID, word)
	docLen := i.DocLen(docID)

	if docCnt == 0 || docLen == 0 {
		return 0
	}

	return float64(docCnt) / float64(docLen)
}

func (i *Index) IDF(docID uint32, word string) float64 {
	wordCnt := float64(i.IndexWordCount(word))
	totalCnt := float64(i.IndexDocCount())

	if wordCnt == 0 || totalCnt == 0 {
		return 0
	}

	return math.Log(totalCnt/wordCnt) + 1
}

func (i *Index) TFIDF(docID uint32, word string) float64 {
	return i.TF(docID, word) * i.IDF(docID, word)
}
