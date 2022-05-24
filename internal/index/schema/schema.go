package schema

import (
	"io/ioutil"
	"os"

	jsoniter "github.com/json-iterator/go"

	"github.com/cyradin/search/internal/index/field"
)

type Field struct {
	Name     string     `json:"name"`
	Type     field.Type `json:"type"`
	Required bool       `json:"required"`

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
