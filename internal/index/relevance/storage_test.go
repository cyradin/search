package relevance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Storage_Add(t *testing.T) {
	data := []struct {
		name string
		data []testStorageData

		indexCnt  map[string]int
		docCnt    map[uint32]map[string]int
		docLen    map[uint32]int
		avgDocLen float64
	}{
		{
			name:     "empty_data",
			indexCnt: make(map[string]int),
			docCnt:   make(map[uint32]map[string]int),
			docLen:   make(map[uint32]int),
		},
		{
			name: "one",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			indexCnt: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
			},
			docLen: map[uint32]int{
				1: 3,
			},
			avgDocLen: 3,
		},
		{
			name: "two",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
			},
			indexCnt: map[string]int{
				"foo": 2,
				"bar": 2,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
				2: {"foo": 1, "bar": 1, "baz": 2},
			},
			docLen: map[uint32]int{
				1: 3, 2: 4,
			},
			avgDocLen: 3.5,
		},
		{
			name: "replace",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 1, words: []string{"bar", "baz", "baz", "baz"}},
			},
			indexCnt: map[string]int{
				"bar": 1,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"baz": 3, "bar": 1},
			},
			docLen: map[uint32]int{
				1: 4,
			},
			avgDocLen: 4,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			storage := NewStorage()
			for _, item := range d.data {
				storage.Add(item.id, item.words)
			}

			require.EqualValues(t, d.indexCnt, storage.wordCounts)
			require.EqualValues(t, d.docCnt, storage.docCounts)
			require.EqualValues(t, d.docLen, storage.docLengths)
			require.Equal(t, d.avgDocLen, storage.AvgDocLen())
		})
	}
}

func Test_Storage_Delete(t *testing.T) {
	data := []struct {
		name   string
		data   []testStorageData
		delete []uint32

		indexCnt  map[string]int
		docCnt    map[uint32]map[string]int
		docLen    map[uint32]int
		avgDocLen float64
	}{
		{
			name:     "empty_data",
			delete:   []uint32{1},
			indexCnt: make(map[string]int),
			docCnt:   make(map[uint32]map[string]int),
			docLen:   make(map[uint32]int),
		},
		{
			name: "one_not_found",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			delete: []uint32{2},
			indexCnt: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			docCnt: map[uint32]map[string]int{
				1: {"foo": 1, "bar": 2},
			},
			docLen: map[uint32]int{
				1: 3,
			},
			avgDocLen: 3,
		},
		{
			name: "one_delete",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
			},
			delete:    []uint32{1},
			indexCnt:  map[string]int{},
			docCnt:    map[uint32]map[string]int{},
			docLen:    map[uint32]int{},
			avgDocLen: 0,
		},
		{
			name: "three_delete_one",
			data: []testStorageData{
				{id: 1, words: []string{"foo", "bar", "bar"}},
				{id: 2, words: []string{"foo", "bar", "baz", "baz"}},
				{id: 3, words: []string{"foo"}},
			},
			delete: []uint32{1},
			indexCnt: map[string]int{
				"foo": 2,
				"bar": 1,
				"baz": 1,
			},
			docCnt: map[uint32]map[string]int{
				2: {"foo": 1, "bar": 1, "baz": 2},
				3: {"foo": 1},
			},
			docLen: map[uint32]int{
				2: 4,
				3: 1,
			},
			avgDocLen: 2.5,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			storage := NewStorage()
			for _, item := range d.data {
				storage.Add(item.id, item.words)
			}

			for _, id := range d.delete {
				storage.Delete(id)
			}

			require.EqualValues(t, d.indexCnt, storage.wordCounts)
			require.EqualValues(t, d.docCnt, storage.docCounts)
			require.EqualValues(t, d.docLen, storage.docLengths)
			require.Equal(t, d.avgDocLen, storage.AvgDocLen())
		})
	}
}
