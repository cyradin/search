package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

var _ Field = (*Bool)(nil)

type Bool struct {
	inner *field[bool]
}

func NewBool() *Bool {
	gf := newField[bool]()
	return &Bool{
		inner: gf,
	}
}

func (f *Bool) Type() schema.Type {
	return schema.TypeBool
}

func (f *Bool) Add(id uint32, value interface{}) {
	v, err := castE[bool](value)
	if err != nil {
		return
	}

	f.inner.Add(id, v)
}

func (f *Bool) Get(value interface{}) *roaring.Bitmap {
	v, err := castE[bool](value)
	if err != nil {
		return roaring.New()
	}

	return f.inner.Get(v)
}

func (f *Bool) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(castSlice[bool](values))
}

func (f *Bool) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
