package field

import (
	"os"

	"github.com/RoaringBitmap/roaring"
	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type Type string

const (
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
}

type StorageData[T any] struct {
	Key   T `json:"key"`
	Value []byte
}

func readField[T comparable](src string) (map[T]*roaring.Bitmap, error) {
	result := make(map[T]*roaring.Bitmap)

	data, err := os.ReadFile(src)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}

	var items []StorageData[T]
	err = json.Unmarshal(data, &items)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		value := new(roaring.Bitmap)
		err := value.UnmarshalBinary(item.Value)
		if err != nil {
			return nil, err
		}

		result[item.Key] = value
	}

	return result, nil
}

func dumpField[T comparable](src string, data map[T]*roaring.Bitmap) error {
	values := make([]StorageData[T], 0, len(data))
	for k, v := range data {
		value, err := v.MarshalBinary()
		if err != nil {
			return err
		}

		values = append(values, StorageData[T]{
			Key:   k,
			Value: value,
		})
	}

	raw, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return os.WriteFile(src, raw, 0644)
}
