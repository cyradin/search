package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Integer)(nil)

type Integer struct {
	inner *field[int32]
}

func NewInteger(ctx context.Context, src string) (*Integer, error) {
	gf, err := newGenericField[int32](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Integer{
		inner: gf,
	}, nil
}

func (f *Integer) Type() Type {
	return TypeInteger
}

func (f *Integer) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Integer) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Integer) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value, cast.ToInt32E)
}

func (f *Integer) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values, cast.ToInt32E)
}
