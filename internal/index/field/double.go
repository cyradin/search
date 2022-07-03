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

func (f *Double) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Double) GetValue(value interface{}) *roaring.Bitmap {
	return f.inner.getValue(value)
}

func (f *Double) GetValuesOr(values []interface{}) *roaring.Bitmap {
	return f.inner.getValuesOr(values)
}

func (f *Double) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Double) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
