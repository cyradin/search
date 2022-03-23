package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type shortValue struct {
	id    uint32
	value int16
	ready chan struct{}
}

type Short struct {
	data map[int16]*roaring.Bitmap
	in   chan shortValue
}

func NewShort(ctx context.Context) *Short {
	result := &Short{
		data: make(map[int16]*roaring.Bitmap),
	}

	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Short) Type() Type {
	return TypeShort
}

func (f *Short) AddValue(id uint32, value interface{}) error {
	return f.addValue(id, value, false)
}

func (f *Short) AddValueSync(id uint32, value interface{}) error {
	return f.addValue(id, value, true)
}

func (f *Short) addValue(id uint32, value interface{}, sync bool) error {
	vv, ok := value.(int16)
	if !ok {
		return fmt.Errorf("required int16, got %s", reflect.TypeOf(value))
	}

	var ready chan struct{}
	if sync {
		ready = make(chan struct{})
		defer close(ready)
	}

	f.in <- shortValue{
		id: id, value: vv, ready: ready,
	}

	if sync {
		<-ready
	}

	return nil
}

func (f *Short) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan shortValue)
		defer close(f.in)
		ready <- struct{}{}

		for {
			select {
			case v := <-f.in:
				f.monitorAdd(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

// monitorAdd add value to index field. Call only from monitor() method
func (f *Short) monitorAdd(v shortValue) {
	var m *roaring.Bitmap
	var ok bool

	m, ok = f.data[v.value]
	if !ok {
		m = roaring.New()
		f.data[v.value] = m
	}

	m.Add(v.id)

	if v.ready != nil {
		v.ready <- struct{}{}
	}
}
