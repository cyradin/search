package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*UnsignedLong)(nil)

type UnsignedLong struct {
	inner *field[uint64]
}

func NewUnsignedLong(ctx context.Context, src string) (*UnsignedLong, error) {
	gf, err := newGenericField[uint64](ctx, src)
	if err != nil {
		return nil, err
	}

	return &UnsignedLong{
		inner: gf,
	}, nil
}

func (f *UnsignedLong) Type() Type {
	return TypeUnsignedLong
}

func (f *UnsignedLong) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *UnsignedLong) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *UnsignedLong) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value, cast.ToUint64E)
}
