package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
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
	gf := newField[bool](cast.ToBoolE)
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

func (f *All) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(true)
}

func (f *All) GetOr(values []interface{}) *roaring.Bitmap {
	return f.Get(true)
}

func (f *All) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *All) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
