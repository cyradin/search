package field

import (
	"context"
	"fmt"
)

type Type string

const (
	TypeKeyword Type = "keyword"
	TypeText    Type = "text"
	TypeBool    Type = "bool"
	TypeSlice   Type = "slice"
	TypeMap     Type = "map"

	TypeUnsignedLong Type = "unsigned_long" // unsigned int64
	TypeLong         Type = "long"          // signed int64
	TypeInteger      Type = "integer"       // signed int32
	TypeShort        Type = "short"         // signed int16
	TypeByte         Type = "byte"          // signed int8
)

func (t Type) Valid() bool {
	return t == TypeKeyword || t == TypeText || t == TypeBool || t == TypeSlice || t == TypeMap
}

type Field interface {
	// Type returns field type
	Type() Type
	// AddValue add document field value
	AddValue(id uint32, value interface{}) error
	// AddValueSync add document field value synchronously
	AddValueSync(id uint32, value interface{}) error
}

func NewField(ctx context.Context, fieldType Type) (Field, error) {
	switch fieldType {
	case TypeBool:
		return NewBool(ctx), nil
	case TypeKeyword:
		return NewKeyword(ctx), nil
	}

	return nil, fmt.Errorf("unknown field type %q", fieldType)
}
