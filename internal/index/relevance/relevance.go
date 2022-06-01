package relevance

type Calculator interface {
	Calculate(docID uint32, word string) float64
}
