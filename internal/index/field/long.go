package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

var _ Field = (*Long)(nil)

type Long struct {
	inner *field[int64]
}

func NewLong() *Long {
	gf := newField[int64](cast.ToInt64E)
	return &Long{
		inner: gf,
	}
}

func (f *Long) Type() schema.Type {
	return schema.TypeLong
}

func (f *Long) Add(id uint32, value interface{}) {
	f.inner.Add(id, value)
}

func (f *Long) Get(value interface{}) *roaring.Bitmap {
	return f.inner.Get(value)
}

func (f *Long) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(values)
}

func (f *Long) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Long) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
