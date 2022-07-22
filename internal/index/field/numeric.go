package field

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

type NumericConstraint interface {
	int8 | int16 | int32 | int64 | uint64 | float32 | float64
}

type Numeric[T NumericConstraint] struct {
	mtx    sync.RWMutex
	data   map[T]*roaring.Bitmap
	values map[uint32][]T
}

func NewNumeric[T NumericConstraint]() *Numeric[T] {
	return &Numeric[T]{
		data:   make(map[T]*roaring.Bitmap),
		values: make(map[uint32][]T),
	}
}

func (f *Numeric[T]) Type() schema.Type {
	var k T
	var result schema.Type
	switch any(k).(type) {
	case int8:
		result = schema.TypeByte
	case int16:
		result = schema.TypeShort
	case int32:
		result = schema.TypeInteger
	case int64:
		result = schema.TypeLong
	case uint64:
		result = schema.TypeUnsignedLong
	case float32:
		result = schema.TypeFloat
	case float64:
		result = schema.TypeDouble
	}

	return result
}

func (f *Numeric[T]) Add(id uint32, value interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	v, err := castE[T](value)
	if err != nil {
		return
	}

	f.values[id] = append(f.values[id], v)

	m, ok := f.data[v]
	if !ok {
		m = roaring.New()
		f.data[v] = m
	}

	m.Add(id)
}

func (f *Numeric[T]) Get(ctx context.Context, value interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	v, err := castE[T](value)
	if err != nil {
		return NewResult(ctx, roaring.New())
	}

	m, ok := f.data[v]
	if !ok {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, m.Clone())
}

func (f *Numeric[T]) GetOr(ctx context.Context, values []interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var result *roaring.Bitmap
	for _, value := range values {
		v, err := castE[T](value)
		if err != nil {
			continue
		}

		m, ok := f.data[v]
		if !ok {
			continue
		}

		if result == nil {
			result = m.Clone()
		} else {
			result.Or(m)
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, result)
}

func (f *Numeric[T]) GetAnd(ctx context.Context, values []interface{}) *Result {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var result *roaring.Bitmap
	for _, value := range values {
		v, err := castE[T](value)
		if err != nil {
			continue
		}

		m, ok := f.data[v]
		if !ok {
			continue
		}

		if result == nil {
			result = m.Clone()
		} else {
			result.And(m)
		}
	}

	if result == nil {
		return NewResult(ctx, roaring.New())
	}

	return NewResult(ctx, result)
}

func (f *Numeric[T]) Delete(id uint32) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	vals, ok := f.values[id]
	if !ok {
		return
	}
	delete(f.values, id)

	for _, v := range vals {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		m.Remove(id)
		if m.GetCardinality() == 0 {
			delete(f.data, v)
		}
	}
}

type numericData[T NumericConstraint] struct {
	Data   map[T]*roaring.Bitmap
	Values map[uint32][]T
}

func (f *Numeric[T]) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(numericData[T]{Data: f.data, Values: f.values})

	return buf.Bytes(), err
}

func (f *Numeric[T]) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	raw := numericData[T]{}
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&raw)
	if err != nil {
		return err
	}
	f.data = raw.Data
	f.values = raw.Values

	return nil
}
