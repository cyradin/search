package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Long)(nil)

type Long struct {
	inner *field[int64]
}

func NewLong(ctx context.Context, src string) (*Long, error) {
	gf, err := newGenericField[int64](ctx, src)
	if err != nil {
		return nil, err
	}

	return &Long{
		inner: gf,
	}, nil
}

func (f *Long) Type() Type {
	return TypeLong
}

func (f *Long) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Long) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Long) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value, cast.ToInt64E)
}
