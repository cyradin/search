package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Analyzer func([]string) []string

type Text struct {
	analyzers []Analyzer
	data      map[string]*roaring.Bitmap
	mtx       sync.RWMutex
}

func NewText(ctx context.Context, analyzers ...Analyzer) *Text {
	return &Text{
		analyzers: analyzers,
		data:      make(map[string]*roaring.Bitmap),
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

	values := []string{value}
	for _, analyzer := range f.analyzers {
		values = analyzer(values)
	}

	for _, v := range values {
		m, ok := f.data[v]
		if !ok {
			m = roaring.New()
			f.data[v] = m
		}

		m.Add(id)
	}
}
