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

func NewBool(ctx context.Context, src string) (*Bool, error) {
	gf, err := newGenericField[bool](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Bool{
		inner: gf,
	}, nil
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
	return f.inner.getValue(value, cast.ToBoolE)
}
