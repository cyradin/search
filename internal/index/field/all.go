package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

const (
	AllField = "_all"
)

var _ Field = (*All)(nil)

// All contains every document in the index.
// This field is necessary to execute queris like { "bool": {}} and {"match_all":{}}
type All struct {
	inner *field[bool]
}

func NewAll(ctx context.Context, src string) (*All, error) {
	gf, err := newGenericField[bool](ctx, src, cast.ToBoolE)
	if err != nil {
		return nil, err
	}

	return &All{
		inner: gf,
	}, nil
}

func (f *All) Type() Type {
	return TypeBool
}

func (f *All) AddValue(id uint32, value interface{}) error {
	return f.inner.AddValue(id, true)
}

func (f *All) AddValueSync(id uint32, value interface{}) error {
	return f.inner.AddValueSync(id, true)
}

func (f *All) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(true)
}

func (f *All) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.GetValue(true)
}
