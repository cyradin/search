package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Float)(nil)

type Float struct {
	inner *field[float32]
}

func NewFloat() *Float {
	gf := newField[float32](cast.ToFloat32E)
	return &Float{
		inner: gf,
	}
}

func (f *Float) Type() schema.Type {
	return schema.TypeFloat
}

func (f *Float) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Float) GetValue(value interface{}) *roaring.Bitmap {
	return f.inner.getValue(value)
}

func (f *Float) GetValuesOr(values []interface{}) *roaring.Bitmap {
	return f.inner.getValuesOr(values)
}

func (f *Float) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Float) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
