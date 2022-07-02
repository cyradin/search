package field

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Scoring(t *testing.T) {
	t.Run("can add one document", func(t *testing.T) {
		index := NewScoring()

		index.Add(1, []string{"foo", "bar", "bar"})

		require.Equal(t, 1, index.IndexDocCount())
		require.Equal(t, 1, index.IndexWordCount("foo"))
		require.Equal(t, 1, index.IndexWordCount("bar"))
		require.Equal(t, 1, index.DocWordCount(1, "foo"))
		require.Equal(t, 2, index.DocWordCount(1, "bar"))
		require.Equal(t, 3, index.DocLen(1))
		require.Equal(t, 3.0, index.AvgDocLen())
	})

	t.Run("can add two documents", func(t *testing.T) {
		index := NewScoring()

		index.Add(1, []string{"foo", "bar", "bar"})
		index.Add(2, []string{"foo", "bar", "baz", "baz"})

		require.Equal(t, 2, index.IndexDocCount())
		require.Equal(t, 2, index.IndexWordCount("foo"))
		require.Equal(t, 2, index.IndexWordCount("bar"))
		require.Equal(t, 1, index.IndexWordCount("baz"))

		require.Equal(t, 1, index.DocWordCount(1, "foo"))
		require.Equal(t, 2, index.DocWordCount(1, "bar"))
		require.Equal(t, 0, index.DocWordCount(1, "baz"))

		require.Equal(t, 1, index.DocWordCount(2, "foo"))
		require.Equal(t, 1, index.DocWordCount(2, "bar"))
		require.Equal(t, 2, index.DocWordCount(2, "baz"))

		require.Equal(t, 3, index.DocLen(1))
		require.Equal(t, 4, index.DocLen(2))
		require.Equal(t, 3.5, index.AvgDocLen())
	})

	t.Run("can replace document", func(t *testing.T) {
		index := NewScoring()

		index.Add(1, []string{"foo", "bar", "bar"})
		index.Add(1, []string{"foo", "baz"})

		require.Equal(t, 1, index.IndexDocCount())
		require.Equal(t, 1, index.IndexWordCount("foo"))
		require.Equal(t, 0, index.IndexWordCount("bar"))
		require.Equal(t, 1, index.IndexWordCount("baz"))

		require.Equal(t, 1, index.DocWordCount(1, "foo"))
		require.Equal(t, 0, index.DocWordCount(1, "bar"))
		require.Equal(t, 1, index.DocWordCount(1, "baz"))

		require.Equal(t, 2, index.DocLen(1))
		require.Equal(t, 2.0, index.AvgDocLen())
	})

	t.Run("can delete document", func(t *testing.T) {
		index := NewScoring()

		index.Add(1, []string{"foo", "bar", "bar"})
		index.Delete(1)

		require.Equal(t, 0, index.IndexDocCount())
		require.Equal(t, 0, index.IndexWordCount("foo"))
		require.Equal(t, 0, index.IndexWordCount("bar"))

		require.Equal(t, 0, index.DocWordCount(1, "foo"))
		require.Equal(t, 0, index.DocWordCount(1, "bar"))

		require.Equal(t, 0, index.DocLen(1))
		require.Equal(t, 0.0, index.AvgDocLen())
	})
}

func Test_Scoring_TF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		index := NewScoring()

		result := index.TF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo"})

		result := index.TF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TF", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo", "bar"})
		index.Add(2, []string{"foo", "baz"})

		result := index.TF(1, "foo")
		assert.Equal(t, 0.5, result)

		result = index.TF(1, "bar")
		assert.Equal(t, 0.5, result)

		result = index.TF(2, "baz")
		assert.Equal(t, 0.5, result)
	})
}

func Test_Scoring_IDF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		index := NewScoring()

		result := index.IDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo"})

		result := index.IDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate IDF", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo", "bar"})
		index.Add(2, []string{"foo", "baz"})

		result := index.IDF(1, "foo")
		assert.Equal(t, 1.0, result)

		result = index.IDF(1, "bar")
		assert.Equal(t, 1.6931471805599454, result)

		result = index.IDF(2, "baz")
		assert.Equal(t, 1.6931471805599454, result)
	})
}

func Test_Scoring_TFIDF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		index := NewScoring()

		result := index.TFIDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo"})

		result := index.TFIDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TFIDF", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo", "bar"})
		index.Add(2, []string{"foo", "baz"})

		result := index.TFIDF(1, "foo")
		assert.Equal(t, 0.5, result)

		result = index.TFIDF(1, "bar")
		assert.Equal(t, 0.8465735902799727, result)

		result = index.TFIDF(2, "baz")
		assert.Equal(t, 0.8465735902799727, result)
	})
}

func Test_Scoring_BM25(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		index := NewScoring()

		result := index.BM25(1, 2.0, 0.75, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo"})

		result := index.BM25(1, 2.0, 0.75, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TFIDF", func(t *testing.T) {
		index := NewScoring()
		index.Add(1, []string{"foo", "bar"})
		index.Add(2, []string{"foo", "baz"})

		result := index.BM25(1, 2.0, 0.75, "foo")
		assert.Equal(t, 0.6, result)

		result = index.BM25(1, 2.0, 0.75, "bar")
		assert.Equal(t, 1.0158883083359673, result)

		result = index.BM25(2, 2.0, 0.75, "baz")
		assert.Equal(t, 1.0158883083359673, result)
	})
}
