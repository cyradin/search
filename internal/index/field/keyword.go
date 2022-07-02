package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	inner *field[string]
}

func NewKeyword() *Keyword {
	gf := newField[string](cast.ToStringE)
	return &Keyword{
		inner: gf,
	}
}

func (f *Keyword) Type() schema.Type {
	return schema.TypeKeyword
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

func (f *Keyword) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
