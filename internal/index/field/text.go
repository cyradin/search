package field

import (
	"context"
	"fmt"
	"reflect"

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

func NewText(ctx context.Context, src string, analyzers ...AnalyzerHandler) *Text {
	gf := newField[string](ctx, src, cast.ToStringE)

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

func (f *Text) AddValue(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		var val string
		return fmt.Errorf("required %s, got %s", reflect.TypeOf(val), reflect.TypeOf(value))
	}
	for _, vv := range f.analyzer([]string{v}) {
		if err := f.inner.AddValue(id, vv); err != nil {
			return err
		}
	}

	return nil
}

func (f *Text) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		var val string
		return fmt.Errorf("required %s, got %s", reflect.TypeOf(val), reflect.TypeOf(value))
	}
	for _, vv := range f.analyzer([]string{v}) {
		if err := f.inner.AddValueSync(id, vv); err != nil {
			return err
		}
	}

	return nil
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
