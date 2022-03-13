package index

import (
	"context"
	"fmt"
)

type IndexWriter struct {
	index *Index

	create chan Document
	delete chan string
}

func NewIndexWriter(index *Index) *IndexWriter {
	return &IndexWriter{
		index: index,

		create: make(chan Document),
		delete: make(chan string),
	}
}

func (w *IndexWriter) Create(doc Document) {
	w.create <- doc
}

func (w *IndexWriter) Delete(id string) {
	w.delete <- id
}

func (w *IndexWriter) Start(ctx context.Context) {
	for {
		select {
		case doc := <-w.create:
			fmt.Println(doc) // @todo create doc
		case id := <-w.delete:
			fmt.Println(id) // @todo delete doc
		case <-ctx.Done():
			return
		}
	}
}
