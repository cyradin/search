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

func NewByte(ctx context.Context, src string) (*Byte, error) {
	gf, err := newGenericField[int8](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Byte{
		inner: gf,
	}, nil
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
	return f.inner.getValue(value, cast.ToInt8E)
}
