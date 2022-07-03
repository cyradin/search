package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Byte)(nil)

type Byte struct {
	inner *field[int8]
}

func NewByte() *Byte {
	gf := newField[int8](cast.ToInt8E)
	return &Byte{
		inner: gf,
	}
}

func (f *Byte) Type() schema.Type {
	return schema.TypeByte
}

func (f *Byte) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Byte) GetValue(value interface{}) *roaring.Bitmap {
	return f.inner.getValue(value)
}

func (f *Byte) GetValuesOr(values []interface{}) *roaring.Bitmap {
	return f.inner.getValuesOr(values)
}

func (f *Byte) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Byte) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
