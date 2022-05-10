package field

import (
	"context"
	"fmt"
	"reflect"
)

type (
	AnalyzerHandler func(next Analyzer) Analyzer
	Analyzer        func([]string) []string

	Text struct {
		analyzer Analyzer
		inner    *field[string]
	}
)

func NewText(ctx context.Context, src string, analyzers ...AnalyzerHandler) (*Text, error) {
	gf, err := newGenericField[string](ctx, src)
	if err != nil {
		return nil, err
	}

	analyzer := func(s []string) []string { return s }
	for i := len(analyzers) - 1; i >= 0; i-- {
		analyzer = analyzers[i](analyzer)
	}

	return &Text{
		inner:    gf,
		analyzer: analyzer,
	}, nil
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
