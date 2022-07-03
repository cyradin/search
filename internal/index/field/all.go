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

func (f *All) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, true)
}

func (f *All) GetValue(value interface{}) *roaring.Bitmap {
	return f.inner.getValue(true)
}

func (f *All) GetValuesOr(values []interface{}) *roaring.Bitmap {
	return f.GetValue(true)
}

func (f *All) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *All) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
