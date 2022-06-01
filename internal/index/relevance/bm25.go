package relevance

var _ Calculator = (*BM25)(nil)

type BM25 struct {
	k float64
	b float64

	tfidf   *TFIDF
	storage *Storage
}

func NewBM25(storage *Storage, k, b float64) *BM25 {
	return &BM25{
		tfidf:   NewTFIDF(storage),
		storage: storage,
		k:       k,
		b:       b,
	}
}

func (b *BM25) Calculate(docID uint32, word string) float64 {
	tf := b.tfidf.TF(docID, word)
	if tf == 0 {
		return 0
	}

	idf := b.tfidf.IDF(docID, word)
	if idf == 0 {
		return 0
	}

	docLen := float64(b.storage.DocLen(docID))
	if docLen == 0 {
		return 0
	}

	return idf * (tf * (b.k + 1)) / (tf + b.k*(1-b.b+b.b*docLen/b.storage.AvgDocLen()))
}
