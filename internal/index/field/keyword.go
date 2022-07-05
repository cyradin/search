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

func (f *Keyword) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Keyword) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Keyword) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Keyword) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
