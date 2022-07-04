package query

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

var _ Query = (*termQuery)(nil)
var _ Query = (*termsQuery)(nil)

type termQuery struct {
	query  Req
	fields Fields
	path   string
}

func newTermQuery(req Req, fields Fields, path string) (*termQuery, error) {
	if err := validation.Validate(req, validation.Required, validation.Length(1, 1)); err != nil {
		return nil, err
	}

	return &termQuery{
		query:  req,
		fields: fields,
		path:   path,
	}, nil
}

func (q *termQuery) exec() (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)

	field, ok := q.fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.Get(val), nil
}

type termsQuery struct {
	query  Req
	fields Fields
	path   string
}

func newTermsQuery(req Req, fields Fields, path string) (*termsQuery, error) {
	err := validation.Validate(req,
		validation.Required,
		validation.Length(1, 1),
		validation.By(func(value interface{}) error {
			key, val := firstVal(req)
			_, err := interfaceToSlice[interface{}](val)
			if err != nil {
				return validation.NewError("query_array_required", fmt.Sprintf("%q must be an array", key))
			}
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}

	return &termsQuery{
		query:  req,
		fields: fields,
		path:   path,
	}, nil
}

func (q *termsQuery) exec() (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)
	values, _ := interfaceToSlice[interface{}](val)

	field, ok := q.fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.GetOr(values), nil
}
