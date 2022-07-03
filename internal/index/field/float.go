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

func (f *Float) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Float) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Float) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Float) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Float) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
