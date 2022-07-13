package field

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type NumericConstraint interface {
	int8 | int16 | int32 | int64 | uint64 | float32 | float64
}

type Numeric[T NumericConstraint] struct {
	mtx  sync.Mutex
	data map[T]*roaring.Bitmap
}

func NewNumeric[T NumericConstraint]() *Numeric[T] {
	result := &Numeric[T]{
		data: make(map[T]*roaring.Bitmap),
	}

	return result
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
	val, err := castE[T](value)
	if err != nil {
		return
	}

	m, ok := f.data[val]
	if !ok {
		m = roaring.New()
		f.data[val] = m
	}

	m.Add(id)

	return
}

func (f *Numeric[T]) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f.data)

	return buf.Bytes(), err
}

func (f *Numeric[T]) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	buf := bytes.NewBuffer(data)

	return gob.NewDecoder(buf).Decode(&f.data)
}

func (f *Numeric[T]) Get(v interface{}) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	val, err := castE[T](v)
	if err != nil {
		return roaring.New()
	}

	vv, ok := f.data[val]
	if !ok {
		return roaring.New()
	}

	return vv.Clone()
}

func (f *Numeric[T]) GetOr(values []interface{}) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var result *roaring.Bitmap
	for _, v := range values {
		val, err := castE[T](v)
		if err != nil {
			continue
		}

		bm, ok := f.data[val]
		if !ok {
			continue
		}

		if result == nil {
			result = bm.Clone()
			continue
		}

		result.Or(bm)
	}

	if result == nil {
		return roaring.New()
	}

	return result
}

func (f *Numeric[T]) GetAnd(values []interface{}) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var result *roaring.Bitmap
	for _, v := range values {
		val, err := castE[T](v)
		if err != nil {
			continue
		}

		bm, ok := f.data[val]
		if !ok {
			continue
		}

		if result == nil {
			result = bm.Clone()
			continue
		}

		result.And(bm)
	}

	if result == nil {
		return roaring.New()
	}

	return result
}

func castSlice[T comparable](values []interface{}) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		v, err := castE[T](value)
		if err != nil {
			continue
		}
		result = append(result, v)
	}
	return result
}

func castE[T comparable](value interface{}) (T, error) {
	var (
		k   T
		val interface{}
		err error
	)

	switch any(k).(type) {
	case bool:
		val, err = cast.ToBoolE(value)
	case int8:
		val, err = cast.ToInt8E(value)
	case int16:
		val, err = cast.ToInt16E(value)
	case int32:
		val, err = cast.ToInt32E(value)
	case int64:
		val, err = cast.ToInt64E(value)
	case uint64:
		val, err = cast.ToUint64E(value)
	case float32:
		val, err = cast.ToFloat32E(value)
	case float64:
		val, err = cast.ToFloat64E(value)
	case string:
		val, err = cast.ToStringE(value)
	}

	if err != nil {
		return k, err
	}

	return val.(T), err
}
