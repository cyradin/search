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

func (f *Bool) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Bool) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Bool) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Bool) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
