package field

import (
	"context"

	"github.com/cyradin/search/internal/index/schema"
)

const (
	AllField = "_all"
)

var _ Field = (*All)(nil)

// All contains every document in the index.
// This field is necessary to execute queris like { "bool": {}} and {"match_all":{}}
type All struct {
	inner *field[bool]
}

func NewAll() *All {
	gf := newField[bool]()
	return &All{
		inner: gf,
	}
}

func (f *All) Type() schema.Type {
	return schema.TypeBool
}

func (f *All) Add(id uint32, value interface{}) {
	f.inner.Add(id, true)
}

func (f *All) Get(ctx context.Context, value interface{}) *Result {
	return NewResult(ctx, f.inner.Get(true))
}

func (f *All) GetOr(ctx context.Context, values []interface{}) *Result {
	return f.Get(ctx, true)
}

func (f *All) GetAnd(ctx context.Context, values []interface{}) *Result {
	return f.Get(ctx, true)
}

func (f *All) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *All) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
