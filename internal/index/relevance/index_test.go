package relevance

import (
	"os"
	"path"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func Test_Index(t *testing.T) {
	t.Run("can load data from file", func(t *testing.T) {
		dir := t.TempDir()

		indexData := indexData{
			AvgDocLen: 100,
		}

		data, err := jsoniter.Marshal(indexData)
		require.NoError(t, err)

		src := path.Join(dir, relevanceFile)
		err = os.WriteFile(src, data, filePermissions)
		require.NoError(t, err)

		index := NewIndex(src)
		err = index.load()
		require.NoError(t, err)

		require.EqualValues(t, index.data, indexData)
	})

	t.Run("can dump data to file", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

		index.data = indexData{
			AvgDocLen: 100,
		}

		err := index.dump()
		require.NoError(t, err)

		_, err = os.Stat(src)
		require.NoError(t, err)

		index2 := NewIndex(src)
		err = index2.load()
		require.NoError(t, err)

		require.EqualValues(t, index.data, index2.data)
	})

	t.Run("can add one document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

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
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

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
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

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
		dir := t.TempDir()
		src := path.Join(dir, relevanceFile)
		index := NewIndex(src)

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
