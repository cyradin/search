package field

import (
	"context"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
)

type unsignedLongValue struct {
	id    uint32
	value uint64
	ready chan struct{}
}

type UnsignedLong struct {
	data map[uint64]*roaring.Bitmap
	in   chan unsignedLongValue
}

func NewUnsignedLong(ctx context.Context) *UnsignedLong {
	result := &UnsignedLong{
		data: make(map[uint64]*roaring.Bitmap),
	}

	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *UnsignedLong) Type() Type {
	return TypeUnsignedLong
}

func (f *UnsignedLong) AddValue(id uint32, value interface{}) error {
	return f.addValue(id, value, false)
}

func (f *UnsignedLong) AddValueSync(id uint32, value interface{}) error {
	return f.addValue(id, value, true)
}

func (f *UnsignedLong) addValue(id uint32, value interface{}, sync bool) error {
	vv, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("required uint64, got %s", reflect.TypeOf(value))
	}

	var ready chan struct{}
	if sync {
		ready = make(chan struct{})
		defer close(ready)
	}

	f.in <- unsignedLongValue{
		id: id, value: vv, ready: ready,
	}

	if sync {
		<-ready
	}

	return nil
}

func (f *UnsignedLong) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan unsignedLongValue)
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
func (f *UnsignedLong) monitorAdd(v unsignedLongValue) {
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
