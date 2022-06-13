package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/spf13/cast"
)

type (
	AnalyzerHandler func(next Analyzer) Analyzer
	Analyzer        func([]string) []string

	Text struct {
		analyzer Analyzer
		inner    *field[string]
	}
)

var _ Field = (*Text)(nil)

func NewText(src string, analyzers ...AnalyzerHandler) *Text {
	gf := newField[string](src, cast.ToStringE)

	analyzer := func(s []string) []string { return s }
	for i := len(analyzers) - 1; i >= 0; i-- {
		analyzer = analyzers[i](analyzer)
	}

	return &Text{
		inner:    gf,
		analyzer: analyzer,
	}
}

func (f *Text) Init() error {
	return f.inner.init()
}

func (f *Text) Type() Type {
	return TypeText
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

func (f *Text) Stop(ctx context.Context) error {
	return f.inner.Stop(ctx)
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
