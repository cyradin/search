package schema

type Type string

const (
	Keyword Type = "keyword"
	Text    Type = "text"
	Bool    Type = "bool"
	Slice   Type = "slice"
	Map     Type = "map"
)

func (t Type) Valid() bool {
	return t == Keyword || t == Text || t == Bool || t == Slice || t == Map
}

type Field struct {
	Name     string
	Type     Type
	Source   string
	Required bool

	Children []Field
}

type Schema struct {
	Fields []Field
}

func New(fields []Field) *Schema {
	return &Schema{
		Fields: fields,
	}
}
