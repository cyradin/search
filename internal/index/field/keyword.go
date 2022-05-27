package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	inner *field[string]
}

func NewKeyword(ctx context.Context, src string) (*Keyword, error) {
	gf := newField[string](ctx, src, cast.ToStringE)
	err := gf.init()
	if err != nil {
		return nil, err
	}

	return &Keyword{
		inner: gf,
	}, nil
}

func (f *Keyword) Type() Type {
	return TypeKeyword
}

func (f *Keyword) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, value)
}

func (f *Keyword) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, value)
}

func (f *Keyword) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Keyword) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}
