package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*UnsignedLong)(nil)

type UnsignedLong struct {
	inner *field[uint64]
}

func NewUnsignedLong() *UnsignedLong {
	gf := newField[uint64](cast.ToUint64E)
	return &UnsignedLong{
		inner: gf,
	}
}

func (f *UnsignedLong) Type() schema.Type {
	return schema.TypeUnsignedLong
}

func (f *UnsignedLong) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *UnsignedLong) GetValue(value interface{}) *roaring.Bitmap {
	return f.inner.getValue(value)
}

func (f *UnsignedLong) GetValuesOr(values []interface{}) *roaring.Bitmap {
	return f.inner.getValuesOr(values)
}

func (f *UnsignedLong) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *UnsignedLong) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
