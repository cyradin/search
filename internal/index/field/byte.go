package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Byte)(nil)

type Byte struct {
	inner *field[int8]
}

func NewByte(ctx context.Context, src string) *Byte {
	gf := newField[int8](ctx, src, cast.ToInt8E)
	return &Byte{
		inner: gf,
	}
}

func (f *Byte) Init() error {
	return f.inner.init()
}

func (f *Byte) Type() Type {
	return TypeByte
}

func (f *Byte) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Byte) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Byte) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Byte) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}
