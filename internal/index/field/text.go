package field

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type (
	Analyzer func([]string) []string

	Text struct {
		analyzer  Analyzer
		inner     *field[string]
		relevance *Relevance
	}
)

var _ Field = (*Text)(nil)

func NewText(analyzer func([]string) []string) *Text {
	gf := newField[string](cast.ToStringE)

	return &Text{
		inner:    gf,
		analyzer: analyzer,
	}
}

func (f *Text) Type() schema.Type {
	return schema.TypeText
}

func (f *Text) AddValue(id uint32, value interface{}) {
	val, err := f.inner.transform(value)
	if err != nil {
		return
	}

	for _, vv := range f.analyzer([]string{val}) {
		f.inner.AddValue(id, vv)
	}
}

func (f *Text) GetValue(value interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValue(value)
}

func (f *Text) GetValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	return f.inner.getValuesOr(values)
}

func (f *Text) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return f.inner.Scores(value, bm)
}

func (f *Text) MarshalBinary() ([]byte, error) {
	return f.inner.MarshalBinary()
}

func (f *Text) UnmarshalBinary(data []byte) error {
	return f.inner.UnmarshalBinary(data)
}
