package field

import (
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

func NewAll(src string) *All {
	gf := newField[bool](src, cast.ToBoolE)
	return &All{
		inner: gf,
	}
}

func (f *All) Init() error {
	return f.inner.init()
}

func (f *All) Type() Type {
	return TypeBool
}

func (f *All) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, true)
}

func (f *All) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(true)
}

func (f *All) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.GetValue(true)
}

func (f *All) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
