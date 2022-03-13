package index

import (
	"time"
)

type Index struct {
	CreatedAt time.Time
	Schema    *Schema
}

func New(Schema *Schema) *Index {
	return &Index{
		CreatedAt: time.Now(),
		Schema:    Schema,
	}
}
