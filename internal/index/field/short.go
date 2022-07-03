package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Short)(nil)

type Short struct {
	inner *field[int16]
}

func NewShort() *Short {
	gf := newField[int16](cast.ToInt16E)
	return &Short{
		inner: gf,
	}
}

func (f *Short) Type() schema.Type {
	return schema.TypeShort
}

func (f *Short) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Short) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Short) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Short) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Short) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
