package schema

import (
	"io/ioutil"
	"os"

	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

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
	Name     string `json:"name"`
	Type     Type   `json:"type"`
	Source   string `json:"source"`
	Required bool   `json:"required"`

	Children []Field `json:"children"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

func New(fields []Field) *Schema {
	return &Schema{
		Fields: fields,
	}
}

func NewFromJSON(data []byte) (*Schema, error) {
	result := new(Schema)
	err := json.Unmarshal(data, result)
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
