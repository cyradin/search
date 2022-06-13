package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Integer)(nil)

type Integer struct {
	inner *field[int32]
}

func NewInteger(src string) *Integer {
	gf := newField[int32](src, cast.ToInt32E)
	return &Integer{
		inner: gf,
	}
}

func (f *Integer) Init() error {
	return f.inner.init()
}

func (f *Integer) Type() Type {
	return TypeInteger
}

func (f *Integer) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Integer) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Integer) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Integer) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
