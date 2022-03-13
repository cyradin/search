package index

type Type string

const (
	Keyword Type = "keyword"
	Text    Type = "text"
	Bool    Type = "bool"
)

type Field struct {
	Type   Type
	Source string
}

type Schema struct {
	Fields map[string]Field
}

func NewSchema() *Schema {
	return &Schema{}
}
