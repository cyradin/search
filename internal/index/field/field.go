package field

import (
	"bytes"
	"context"
	"encoding"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/spf13/cast"
)

type FieldData struct {
	Type     schema.Type
	Analyzer func([]string) []string
	Scoring  *Scoring
}

type Field interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	// Type returns field type
	Type() schema.Type
	// Add add document field value
	Add(id uint32, value interface{})
	// Get get bitmap clone by value
	Get(ctx context.Context, value interface{}) *Result
	// GetOr compute the union between bitmaps of the passed values
	GetOr(ctx context.Context, values []interface{}) *Result
	// GetAnd compute the intersection between bitmaps of the passed values
	GetAnd(ctx context.Context, values []interface{}) *Result
	// Delete document field values
	Delete(id uint32)
}

type Score struct {
	ID    uint32
	Value float64
}

type field[T comparable] struct {
	mtx  sync.Mutex
	data map[T]*roaring.Bitmap
}

func newField[T comparable]() *field[T] {
	result := &field[T]{
		data: make(map[T]*roaring.Bitmap),
	}

	return result
}

func (f *field[T]) Add(id uint32, value T) {
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

func (f *field[T]) MarshalBinary() ([]byte, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(f.data)

	return buf.Bytes(), err
}

func (f *field[T]) UnmarshalBinary(data []byte) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	buf := bytes.NewBuffer(data)

	return gob.NewDecoder(buf).Decode(&f.data)
}

func (f *field[T]) Get(value T) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	vv, ok := f.data[value]
	if !ok {
		return roaring.New()
	}

	return vv.Clone()
}

func (f *field[T]) GetOr(values []T) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var result *roaring.Bitmap
	for _, value := range values {
		bm, ok := f.data[value]
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

func (f *field[T]) GetAnd(values []T) *roaring.Bitmap {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var result *roaring.Bitmap
	for _, value := range values {
		bm, ok := f.data[value]
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

func New(f FieldData) (Field, error) {
	var field Field

	switch f.Type {
	case schema.TypeAll:
		field = NewAll()
	case schema.TypeBool:
		field = NewBool()
	case schema.TypeKeyword:
		field = NewKeyword()
	case schema.TypeText:
		if f.Scoring == nil {
			return nil, fmt.Errorf("field scoring data required, but not provided")
		}
		field = NewText(f.Analyzer, f.Scoring)
	// @todo implement slice type
	// case schema.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice()
	// @todo implement map type
	// case schema.TypeNap:
	// 	i.fields[f.Name] = field.NewMap()
	case schema.TypeUnsignedLong:
		field = NewNumeric[uint64]()
	case schema.TypeLong:
		field = NewNumeric[int64]()
	case schema.TypeInteger:
		field = NewNumeric[int32]()
	case schema.TypeShort:
		field = NewNumeric[int16]()
	case schema.TypeByte:
		field = NewNumeric[int8]()
	case schema.TypeDouble:
		field = NewNumeric[float64]()
	case schema.TypeFloat:
		field = NewNumeric[float32]()
	default:
		return nil, fmt.Errorf("invalid field type %q", f.Type)
	}

	return field, nil
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

func sliceToInterfaceSlice[T comparable](data []T) []interface{} {
	result := make([]interface{}, len(data))
	for i, v := range data {
		result[i] = v
	}
	return result
}
