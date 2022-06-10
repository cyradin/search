package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Float)(nil)

type Float struct {
	inner *field[float32]
}

func NewFloat(ctx context.Context, src string) *Float {
	gf := newField[float32](ctx, src, cast.ToFloat32E)
	return &Float{
		inner: gf,
	}
}

func (f *Float) Init() error {
	return f.inner.init()
}

func (f *Float) Type() Type {
	return TypeFloat
}

func (f *Float) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Float) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Float) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Float) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
