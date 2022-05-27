package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Double)(nil)

type Double struct {
	inner *field[float64]
}

func NewDouble(ctx context.Context, src string) (*Double, error) {
	gf := newField[float64](ctx, src, cast.ToFloat64E)
	err := gf.init()
	if err != nil {
		return nil, err
	}

	return &Double{
		inner: gf,
	}, nil
}

func (f *Double) Type() Type {
	return TypeDouble
}

func (f *Double) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Double) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Double) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Double) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}
