package events

import "github.com/cyradin/search/internal/index/schema"

type IndexAdd struct {
	Name   string
	Schema schema.Schema
}

func NewIndexAdd(name string, schema schema.Schema) IndexAdd {
	return IndexAdd{
		Name:   name,
		Schema: schema,
	}
}

func (e IndexAdd) Code() string {
	return "index.add"
}

type IndexDelete struct {
	Name string
}

func NewIndexDelete(name string) IndexDelete {
	return IndexDelete{Name: name}
}

func (e IndexDelete) Code() string {
	return "index.delete"
}
