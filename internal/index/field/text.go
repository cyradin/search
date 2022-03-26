package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type (
	AnalyzerHandler func(next Analyzer) Analyzer
	Analyzer        func([]string) []string

	Text struct {
		analyzer Analyzer
		data     map[string]*roaring.Bitmap
		mtx      sync.RWMutex
	}
)

func NewText(ctx context.Context, analyzers ...AnalyzerHandler) *Text {
	analyzer := func(s []string) []string { return s }
	for i := len(analyzers) - 1; i >= 0; i-- {
		analyzer = analyzers[i](analyzer)
	}

	return &Text{
		analyzer: analyzer,
		data:     make(map[string]*roaring.Bitmap),
	}
}

func (f *Text) Type() Type {
	return TypeText
}

func (f *Text) AddValue(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("required string, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Text) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("required string, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Text) addValue(id uint32, value string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	values := f.analyzer([]string{value})
	for _, v := range values {
		m, ok := f.data[v]
		if !ok {
			m = roaring.New()
			f.data[v] = m
		}

		m.Add(id)
	}
}
