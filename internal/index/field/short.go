package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Short)(nil)

type Short struct {
	inner *field[int16]
}

func NewShort(ctx context.Context, src string) *Short {
	gf := newField[int16](ctx, src, cast.ToInt16E)
	return &Short{
		inner: gf,
	}
}

func (f *Short) Init() error {
	return f.inner.init()
}

func (f *Short) Type() Type {
	return TypeShort
}

func (f *Short) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Short) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Short) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Short) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Short) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
