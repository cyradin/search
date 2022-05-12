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

func NewFloat(ctx context.Context, src string) (*Float, error) {
	gf, err := newGenericField[float32](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Float{
		inner: gf,
	}, nil
}

func (f *Float) Type() Type {
	return TypeFloat
}

func (f *Float) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Float) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Float) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value, cast.ToFloat32E)
}

func (f *Float) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values, cast.ToFloat32E)
}
