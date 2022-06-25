package schema

import (
	"io/ioutil"
	"os"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
)

type Schema struct {
	Fields map[string]Field `json:"fields"`
}

func New(fields map[string]Field) Schema {
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

func (s Schema) ValidateDoc(doc map[string]interface{}) error {
	return ValidateDoc(s, doc)
}

func (s Schema) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Fields, validation.Required),
	)
}
