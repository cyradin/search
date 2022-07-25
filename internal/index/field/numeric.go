package field

import (
	"bytes"
	"context"
	"encoding/gob"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

type NumericConstraint interface {
	int8 | int16 | int32 | int64 | uint64 | float32 | float64
}

type Numeric[T NumericConstraint] struct {
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

func (f *Numeric[T]) Term(ctx context.Context, value interface{}) *Result {
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

func (f *Numeric[T]) Match(ctx context.Context, value interface{}) *Result {
	return f.Term(ctx, value)
}

func (f *Numeric[T]) Delete(id uint32) {
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

func (f *Numeric[T]) Data(id uint32) []interface{} {
	var result []interface{}

	for _, v := range f.values[id] {
		m, ok := f.data[v]
		if !ok {
			continue
		}
		if m.Contains(id) {
			result = append(result, v)
		}
	}

	return result
}

type numericData[T NumericConstraint] struct {
	Data   map[T]*roaring.Bitmap
	Values map[uint32][]T
}

func (f *Numeric[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(numericData[T]{Data: f.data, Values: f.values})

	return buf.Bytes(), err
}

func (f *Numeric[T]) UnmarshalBinary(data []byte) error {
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
