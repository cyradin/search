package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	inner *field[string]
}

func NewKeyword(src string) *Keyword {
	gf := newField[string](src, cast.ToStringE)
	return &Keyword{
		inner: gf,
	}
}

func (f *Keyword) Init() error {
	return f.inner.init()
}

func (f *Keyword) Type() Type {
	return TypeKeyword
}

func (f *Keyword) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Keyword) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Keyword) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Keyword) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
