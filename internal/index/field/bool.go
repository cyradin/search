package field

import (
	"context"

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

func (f *Bool) Get(ctx context.Context, value interface{}) *Result {
	v, err := castE[bool](value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, f.inner.Get(v))
}

func (f *Bool) GetOr(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, f.inner.GetOr(castSlice[bool](values)))
}

func (f *Bool) GetAnd(ctx context.Context, values []interface{}) *Result {
	return NewResult(ctx, f.inner.GetAnd(castSlice[bool](values)))
}

func (f *Bool) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Bool) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
