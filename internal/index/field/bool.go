package field

import (
	"context"

	"github.com/RoaringBitmap/roaring"
)

type boolValue struct {
	id    uint32
	value bool
	ready chan struct{}
}

type Bool struct {
	data map[bool]*roaring.Bitmap

	in chan boolValue
}

func NewBool(ctx context.Context) *Bool {
	result := &Bool{
		data: make(map[bool]*roaring.Bitmap),
	}
	ready := make(chan struct{})
	result.monitor(ctx, ready)
	<-ready // wait until monitor is ready

	return result
}

func (f *Bool) Type() Type {
	return TypeBool
}

func (f *Bool) Set(id uint32, value bool) {
	f.in <- boolValue{
		id: id, value: value,
	}
}

func (f *Bool) SetSync(id uint32, value bool) {
	ready := make(chan struct{})
	f.in <- boolValue{
		id: id, value: value, ready: ready,
	}
	<-ready
}

func (f *Bool) monitor(ctx context.Context, ready chan<- struct{}) {
	go func(ctx context.Context) {
		f.in = make(chan boolValue)
		defer close(f.in)
		ready <- struct{}{}

		for {
			select {
			case v := <-f.in:
				f.doAdd(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

// doAdd add value to index field. Call only from monitor() method
func (f *Bool) doAdd(v boolValue) {
	m, ok := f.data[v.value]
	if !ok {
		m = roaring.New()
		f.data[v.value] = m
	}

	m.Add(v.id)

	if v.ready != nil {
		v.ready <- struct{}{}
	}
}
