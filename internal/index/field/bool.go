package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	inner *field[bool]
}

func NewBool(src string) *Bool {
	gf := newField[bool](src, cast.ToBoolE)
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

func (f *Bool) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
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
