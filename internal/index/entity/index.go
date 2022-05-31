package entity

import (
	"time"

	"github.com/cyradin/search/internal/index/schema"
)

type Index struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Schema    schema.Schema `json:"schema"`
}

func NewIndex(name string, s schema.Schema) Index {
	return Index{
		Name:      name,
		CreatedAt: time.Now(),
		Schema:    s,
	}
}

type DocSource map[string]interface{}
