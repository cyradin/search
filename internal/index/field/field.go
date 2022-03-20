package field

type Type string

const (
	TypeKeyword Type = "keyword"
	TypeText    Type = "text"
	TypeBool    Type = "bool"
	TypeSlice   Type = "slice"
	TypeMap     Type = "map"
)

func (t Type) Valid() bool {
	return t == TypeKeyword || t == TypeText || t == TypeBool || t == TypeSlice || t == TypeMap
}

type Field interface {
	Type() Type
}
