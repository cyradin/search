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
