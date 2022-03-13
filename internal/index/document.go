package index

type FieldValue struct {
	Field Field
	Value interface{}
}

type Document struct {
	ID     string
	Fields map[string]FieldValue
}
