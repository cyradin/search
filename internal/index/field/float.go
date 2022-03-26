package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Float struct {
	data map[float32]*roaring.Bitmap
	mtx  sync.RWMutex
}

func NewFloat(ctx context.Context) *Float {
	return &Float{
		data: make(map[float32]*roaring.Bitmap),
	}
}

func (f *Float) Type() Type {
	return TypeFloat
}

func (f *Float) AddValue(id uint32, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("required float32, got %s", reflect.TypeOf(value))
	}
	go f.addValue(id, v)
	return nil
}

func (f *Float) AddValueSync(id uint32, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("required float32, got %s", reflect.TypeOf(value))
	}
	f.addValue(id, v)
	return nil
}

func (f *Float) addValue(id uint32, value float32) {
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
