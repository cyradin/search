package field

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/schema"
)

type FieldData struct {
	Type     schema.Type
	Analyzer func([]string) []string
}

type Field interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	// Type returns field type
	Type() schema.Type
	// AddValue add document field value
	AddValue(id uint32, value interface{})
	// GetValue get bitmap clone by value
	GetValue(value interface{}) (*roaring.Bitmap, bool)
	// GetValuesOr compute the union between bitmaps of the passed values
	GetValuesOr(values []interface{}) (*roaring.Bitmap, bool)
	// GetValuesAnd compute the intersection between bitmaps of the passed values
	// GetValuesAnd(values []interface{}) (*roaring.Bitmap, bool)
	// Scores calculate doc scores
	Scores(value interface{}, bm *roaring.Bitmap) Scores
}

type Score struct {
	ID    uint32
	Value float64
}

type Scores []Score

type field[T comparable] struct {
	mtx       sync.Mutex
	data      map[T]*roaring.Bitmap
	transform func(interface{}) (T, error)
}

func newField[T comparable](transformer func(interface{}) (T, error)) *field[T] {
	result := &field[T]{
		data:      make(map[T]*roaring.Bitmap),
		transform: transformer,
	}

	return result
}

func (f *field[T]) AddValue(id uint32, value interface{}) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	val, err := f.transform(value)
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

func (f *field[T]) Scores(value interface{}, bm *roaring.Bitmap) Scores {
	return nil
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
		field = NewText(f.Analyzer) // @todo pass analyzers from schema
	// @todo implement slice type
	// case schema.TypeSlice:
	// 	i.fields[f.Name] = field.NewSlice()
	// @todo implement map type
	// case schema.TypeNap:
	// 	i.fields[f.Name] = field.NewMap()
	case schema.TypeUnsignedLong:
		field = NewUnsignedLong()
	case schema.TypeLong:
		field = NewLong()
	case schema.TypeInteger:
		field = NewInteger()
	case schema.TypeShort:
		field = NewShort()
	case schema.TypeByte:
		field = NewByte()
	case schema.TypeDouble:
		field = NewDouble()
	case schema.TypeFloat:
		field = NewFloat()
	default:
		return nil, fmt.Errorf("invalid field type %q", f.Type)
	}

	return field, nil
}
