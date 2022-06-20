package source

import (
	"os"
	"path"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func newTestDoc1() Document {
	return Document{
		ID: 1,
		Source: map[string]interface{}{
			"id":   "1",
			"name": "foo",
			"properties": map[string]interface{}{
				"colors": []interface{}{"red", "blue"},
			},
		},
	}
}

func newTestDoc2() Document {
	return Document{
		ID: 2,
		Source: map[string]interface{}{
			"id":   "1",
			"name": "foo",
			"properties": map[string]interface{}{
				"colors": []interface{}{"red", "blue"},
			},
		},
	}
}

func Test_Index(t *testing.T) {
	t.Run("can create new index", func(t *testing.T) {
		dir := t.TempDir()

		index, err := NewIndex(dir)
		require.NoError(t, err)
		require.NotNil(t, index.docs)
	})

	t.Run("can load data from file", func(t *testing.T) {
		dir := t.TempDir()

		doc1 := newTestDoc1()
		docs := map[uint32]Document{
			doc1.ID: doc1,
		}
		data, err := jsoniter.Marshal(docs)
		require.NoError(t, err)

		src := path.Join(dir, sourceFile)
		err = os.WriteFile(src, data, filePermissions)
		require.NoError(t, err)

		index, err := NewIndex(src)
		require.NoError(t, err)
		err = index.load()
		require.NoError(t, err)

		doc2, ok := index.docs[doc1.ID]
		require.True(t, ok)
		require.EqualValues(t, doc1, doc2)
	})

	t.Run("can dump data to file", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc1 := newTestDoc1()
		index.docs[doc1.ID] = doc1

		err = index.dump()
		require.NoError(t, err)

		_, err = os.Stat(src)
		require.NoError(t, err)

		index2, err := NewIndex(src)
		require.NoError(t, err)
		err = index2.load()
		require.NoError(t, err)

		doc2, ok := index.docs[doc1.ID]
		require.True(t, ok)
		require.EqualValues(t, doc1, doc2)
	})

	t.Run("can insert document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		id, err := index.Insert(doc.ID, doc.Source)
		require.NoError(t, err)
		require.Equal(t, doc.ID, id)

		require.EqualValues(t, index.docs[1], doc)
	})

	t.Run("can generate document id", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		id, err := index.Insert(0, doc.Source)
		require.NoError(t, err)
		require.Equal(t, uint32(1), id)
		require.EqualValues(t, index.docs[1], doc)
	})

	t.Run("cannot insert existing document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		_, err = index.Insert(doc.ID, doc.Source)
		require.NoError(t, err)
		_, err = index.Insert(doc.ID, doc.Source)
		require.Error(t, err)
	})

	t.Run("can get existing document by id", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		_, err = index.Insert(doc.ID, doc.Source)
		require.NoError(t, err)

		doc2, err := index.One(doc.ID)
		require.NoError(t, err)
		require.EqualValues(t, doc, doc2)
	})

	t.Run("cannot get unexisting document by id", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		_, err = index.One(1)
		require.Error(t, err)
	})

	t.Run("can get multiple documents by ids", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc1 := newTestDoc1()
		doc2 := newTestDoc2()

		_, err = index.Insert(doc1.ID, doc1.Source)
		require.NoError(t, err)
		_, err = index.Insert(doc2.ID, doc2.Source)
		require.NoError(t, err)

		docs, err := index.Multi(doc1.ID, doc2.ID)
		require.NoError(t, err)
		require.ElementsMatch(t, []Document{doc1, doc2}, docs)
	})

	t.Run("can get all documents", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc1 := newTestDoc1()
		doc2 := newTestDoc2()
		_, err = index.Insert(doc1.ID, doc1.Source)
		require.NoError(t, err)
		_, err = index.Insert(doc2.ID, doc2.Source)
		require.NoError(t, err)

		ch, _ := index.All()

		var result []Document
		for doc := range ch {
			result = append(result, doc)
		}
		require.ElementsMatch(t, []Document{doc1, doc2}, result)
	})

	t.Run("can update existing document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		_, err = index.Insert(doc.ID, doc.Source)
		require.NoError(t, err)

		doc2 := newTestDoc1()
		doc2.Source["vvv"] = "aaa"
		err = index.Update(doc2.ID, doc2.Source)
		require.NoError(t, err)
		require.EqualValues(t, index.docs[doc.ID], doc2)
	})

	t.Run("cannot update non-existing document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		err = index.Update(doc.ID, doc.Source)
		require.Error(t, err)
	})

	t.Run("cannot update document with invalid id", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		err = index.Update(0, doc.Source)
		require.Error(t, err)
	})

	t.Run("can delete existing document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		doc := newTestDoc1()
		_, err = index.Insert(doc.ID, doc.Source)
		require.NoError(t, err)

		err = index.Delete(doc.ID)
		require.NoError(t, err)
		require.EqualValues(t, index.docs[doc.ID], Document{})
	})

	t.Run("cannot delete non-existing document", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		err = index.Delete(1)
		require.Error(t, err)
	})

	t.Run("cannot delete document with invalid id", func(t *testing.T) {
		dir := t.TempDir()
		src := path.Join(dir, sourceFile)
		index, err := NewIndex(src)
		require.NoError(t, err)

		err = index.Delete(0)
		require.Error(t, err)
	})
}
