package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	inner *field[bool]
}

func NewBool() *Bool {
	gf := newField[bool](cast.ToBoolE)
	return &Bool{
		inner: gf,
	}
}

func (f *Bool) Type() schema.Type {
	return schema.TypeBool
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

func (f *Bool) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
