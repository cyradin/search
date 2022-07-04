package query

import (
	"github.com/RoaringBitmap/roaring"
)

var _ Query = (*termQuery)(nil)
var _ Query = (*termsQuery)(nil)

type termQuery struct {
	query  Req
	fields Fields
	path   string
}

func newTermQuery(req Req, fields Fields, path string) (*termQuery, error) {
	if len(req) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(req) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
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
	if len(req) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), path)
	}
	if len(req) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), path)
	}

	return &termsQuery{
		query:  req,
		fields: fields,
		path:   path,
	}, nil
}

func (q *termsQuery) exec() (*roaring.Bitmap, error) {
	key, val := firstVal(q.query)

	values, err := interfaceToSlice[interface{}](val)
	if err != nil {
		return nil, NewErrSyntax(errMsgArrayValueRequired(), pathJoin(q.path, key))
	}

	field, ok := q.fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.GetOr(values), nil
}
