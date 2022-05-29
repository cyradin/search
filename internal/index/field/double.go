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

func NewDouble(ctx context.Context, src string) *Double {
	gf := newField[float64](ctx, src, cast.ToFloat64E)
	return &Double{
		inner: gf,
	}
}

func (f *Double) Init() error {
	return f.inner.init()
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

func (f *Double) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
