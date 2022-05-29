package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	inner *field[bool]
}

func NewBool(ctx context.Context, src string) *Bool {
	gf := newField[bool](ctx, src, cast.ToBoolE)
	return &Bool{
		inner: gf,
	}
}

func (f *Bool) Init() error {
	return f.inner.init()
}

func (f *Bool) Type() Type {
	return TypeBool
}

func (f *Bool) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Bool) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Bool) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Bool) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Bool) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
