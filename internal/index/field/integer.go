package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Integer)(nil)

type Integer struct {
	inner *field[int32]
}

func NewInteger() *Integer {
	gf := newField[int32](cast.ToInt32E)
	return &Integer{
		inner: gf,
	}
}

func (f *Integer) Type() schema.Type {
	return schema.TypeInteger
}

func (f *Integer) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Integer) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Integer) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Integer) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Integer) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
