package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type integerValue struct {
	id    uint32
	value int32
	ready chan struct{}
}

type Integer struct {
	data map[int32]*roaring.Bitmap
	in   chan integerValue
}

func NewInteger(ctx context.Context) *Integer {
	result := &Integer{
		data: make(map[int32]*roaring.Bitmap),
	}

	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Integer) Type() Type {
	return TypeInteger
}

func (f *Integer) AddValue(id uint32, value interface{}) error {
	return f.addValue(id, value, false)
}

func (f *Integer) AddValueSync(id uint32, value interface{}) error {
	return f.addValue(id, value, true)
}

func (f *Integer) addValue(id uint32, value interface{}, sync bool) error {
	vv, ok := value.(int32)
	if !ok {
		return fmt.Errorf("required int32, got %s", reflect.TypeOf(value))
	}

	var ready chan struct{}
	if sync {
		ready = make(chan struct{})
		defer close(ready)
	}

	f.in <- integerValue{
		id: id, value: vv, ready: ready,
	}

	if sync {
		<-ready
	}

	return nil
}

func (f *Integer) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan integerValue)
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
func (f *Integer) monitorAdd(v integerValue) {
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
