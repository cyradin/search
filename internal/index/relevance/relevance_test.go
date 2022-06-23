package relevance

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_TF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

		result := index.TF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
		index.Add(1, []string{"foo"})

		result := index.TF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TF", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
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

func Test_IDF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

		result := index.IDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
		index.Add(1, []string{"foo"})

		result := index.IDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate IDF", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
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

func Test_TFIDF(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

		result := index.TFIDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
		index.Add(1, []string{"foo"})

		result := index.TFIDF(1, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TFIDF", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
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

func Test_BM25(t *testing.T) {
	t.Run("must return 0 if no documents", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

		result := index.BM25(1, 2.0, 0.75, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("must return 0 if no word in index", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
		index.Add(1, []string{"foo"})

		result := index.BM25(1, 2.0, 0.75, "word")
		require.Equal(t, 0.0, result)
	})

	t.Run("can properly calculate TFIDF", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)
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
