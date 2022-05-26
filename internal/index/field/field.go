package field

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/pkg/finisher"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type Type string

const (
	TypeAll  Type = "all"
	TypeBool Type = "bool"

	// String types
	TypeKeyword Type = "keyword"
	TypeText    Type = "text"

	TypeSlice Type = "slice"
	TypeMap   Type = "map"

	// Integer types
	TypeUnsignedLong Type = "unsigned_long" // unsigned int64
	TypeLong         Type = "long"          // signed int64
	TypeInteger      Type = "integer"       // signed int32
	TypeShort        Type = "short"         // signed int16
	TypeByte         Type = "byte"          // signed int8

	// Float types
	TypeDouble Type = "double" // float64
	TypeFloat  Type = "float"  // float32
)

func (t Type) Valid() bool {
	return t == TypeBool ||
		t == TypeKeyword ||
		t == TypeText ||
		t == TypeSlice ||
		t == TypeMap ||
		t == TypeUnsignedLong ||
		t == TypeLong ||
		t == TypeInteger ||
		t == TypeShort ||
		t == TypeByte ||
		t == TypeDouble ||
		t == TypeFloat
}

type Field interface {
	// Type returns field type
	Type() Type
	// AddValue add document field value
	AddValue(id uint32, value interface{}) error
	// AddValueSync add document field value synchronously
	AddValueSync(id uint32, value interface{}) error
	// GetValue get bitmap clone by value
	GetValue(value interface{}) (*roaring.Bitmap, bool)
	// GetValuesOr compute the union between bitmaps of the passed values
	GetValuesOr(values []interface{}) (*roaring.Bitmap, bool)
	// GetValuesAnd compute the intersection between bitmaps of the passed values
	// GetValuesAnd(values []interface{}) (*roaring.Bitmap, bool)
}

type field[T comparable] struct {
	src string

	mtx       sync.Mutex
	data      map[T]*roaring.Bitmap
	transform func(interface{}) (T, error)
}

func newGenericField[T comparable](ctx context.Context, src string, transformer func(interface{}) (T, error)) (*field[T], error) {
	result := &field[T]{
		src:       src,
		transform: transformer,
	}
	if err := result.load(); err != nil {
		return nil, err
	}
	finisher.Add(result)

	return result, nil
}

func (f *field[T]) AddValue(id uint32, value interface{}) error {
	v, err := f.transform(value)
	if err != nil {
		return err
	}
	go f.addValue(id, v)
	return nil
}

func (f *field[T]) AddValueSync(id uint32, value interface{}) error {
	v, err := f.transform(value)
	if err != nil {
		return err
	}
	f.addValue(id, v)
	return nil
}

func (f *field[T]) addValue(id uint32, value T) {
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

func (f *field[T]) Stop(ctx context.Context) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return f.dump()
}

func (f *field[T]) load() error {
	f.data = make(map[T]*roaring.Bitmap)

	data, err := os.ReadFile(f.src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&f.data); err != nil {
		return err
	}

	return nil
}

func (f *field[T]) dump() error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(f.data); err != nil {
		return err
	}

	return os.WriteFile(f.src, buf.Bytes(), 0644)
}

func (f *field[T]) getValue(v interface{}) (*roaring.Bitmap, bool) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	val, err := f.transform(v)
	if err != nil {
		return nil, false
	}

	vv, ok := f.data[val]
	if !ok {
		return nil, false
	}

	return vv.Clone(), true
}

func (f *field[T]) getValuesOr(values []interface{}) (*roaring.Bitmap, bool) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var result *roaring.Bitmap
	for _, v := range values {
		val, err := f.transform(v)
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

	return result, result != nil
}
