package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Double)(nil)

type Double struct {
	inner *field[float64]
}

func NewDouble() *Double {
	gf := newField[float64](cast.ToFloat64E)
	return &Double{
		inner: gf,
	}
}

func (f *Double) Type() schema.Type {
	return schema.TypeDouble
}

func (f *Double) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Double) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Double) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Double) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Double) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
