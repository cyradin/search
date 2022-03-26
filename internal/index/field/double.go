package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Double struct {
	data map[float64]*roaring.Bitmap
	mtx  sync.RWMutex
}

func NewDouble(ctx context.Context) *Double {
	return &Double{
		data: make(map[float64]*roaring.Bitmap),
	}
}

func (f *Double) Type() Type {
	return TypeDouble
}

func (f *Double) AddValue(id uint32, value interface{}) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("required float64, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Double) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("required float64, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Double) addValue(id uint32, value float64) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	m, ok := f.data[value]
	if !ok {
		m = roaring.New()
		f.data[value] = m
	}

	m.Add(id)

	return
}
