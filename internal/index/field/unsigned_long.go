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

func NewUnsignedLong(ctx context.Context, src string) *UnsignedLong {
	gf := newField[uint64](ctx, src, cast.ToUint64E)
	return &UnsignedLong{
		inner: gf,
	}
}

func (f *UnsignedLong) Init() error {
	return f.inner.init()
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
	return f.inner.getValue(value)
}

func (f *UnsignedLong) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *UnsignedLong) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
