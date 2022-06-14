package schema

import (
	"io/ioutil"
	"os"

	jsoniter "github.com/json-iterator/go"
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

type Field struct {
	Name     string `json:"name"`
	Type     Type   `json:"type"`
	Required bool   `json:"required"`

	Children []Field `json:"children"`
}

func NewField(name string, fieldType Type, required bool, children ...Field) Field {
	return Field{
		Name:     name,
		Type:     fieldType,
		Required: required,
		Children: children,
	}
}

type Schema struct {
	Fields []Field `json:"fields"`
}

func New(fields []Field) Schema {
	return Schema{
		Fields: fields,
	}
}

func NewFromJSON(data []byte) (*Schema, error) {
	result := new(Schema)
	err := jsoniter.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func NewFromFile(src string) (*Schema, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return NewFromJSON(data)
}
