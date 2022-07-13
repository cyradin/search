package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

var _ Field = (*Keyword)(nil)

type Keyword struct {
	inner *field[string]
}

func NewKeyword() *Keyword {
	gf := newField[string]()
	return &Keyword{
		inner: gf,
	}
}

func (f *Keyword) Type() schema.Type {
	return schema.TypeKeyword
}

func (f *Keyword) Add(id uint32, value interface{}) {
	v, err := castE[string](value)
	if err != nil {
		return
	}

	f.inner.Add(id, v)
}

func (f *Keyword) Get(value interface{}) *roaring.Bitmap {
	v, err := castE[string](value)
	if err != nil {
		return roaring.New()
	}

	return f.inner.Get(v)
}

func (f *Keyword) GetOr(values []interface{}) *roaring.Bitmap {
	return f.inner.GetOr(castSlice[string](values))
}

func (f *Keyword) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Keyword) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
