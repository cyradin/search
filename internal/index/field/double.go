package field

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
)

type Double struct {
	data map[float64]*roaring.Bitmap
	mtx  sync.RWMutex
	src  string
}

func NewDouble(ctx context.Context, src string) (*Double, error) {
	data, err := readField[float64](src)
	if err != nil {
		return nil, err
	}

	result := &Double{
		data: data,
		src:  src,
	}
	finisher.Add(result)

	return result, nil
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

func (f *Double) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return dumpField(f.src, f.data)
}
