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

func NewDouble(src string) *Double {
	gf := newField[float64](src, cast.ToFloat64E)
	return &Double{
		inner: gf,
	}
}

func (f *Double) Init() error {
	return f.inner.init()
}

func (f *Double) Type() schema.Type {
	return schema.TypeDouble
}

func (f *Double) AddValue(id uint32, value interface{}) {
	f.inner.AddValue(id, value)
}

func (f *Double) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Double) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Double) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}
