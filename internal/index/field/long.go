package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Long)(nil)

type Long struct {
	inner *field[int64]
}

func NewLong(src string) *Long {
	gf := newField[int64](src, cast.ToInt64E)
	return &Long{
		inner: gf,
	}
}

func (f *Long) Init() error {
	return f.inner.init()
}

func (f *Long) Type() Type {
	return TypeLong
}

func (f *Long) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Long) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Long) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Long) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
