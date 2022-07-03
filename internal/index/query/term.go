package query

import (
	"github.com/RoaringBitmap/roaring"
)

var _ query = (*termQuery)(nil)
var _ query = (*termsQuery)(nil)

type termQuery struct {
	params queryParams
}

func newTermQuery(params queryParams) (*termQuery, error) {
	if len(params.data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), params.path)
	}
	if len(params.data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), params.path)
	}

	return &termQuery{
		params: params,
	}, nil
}

func (q *termQuery) exec() (*roaring.Bitmap, error) {
	key, val := firstVal(q.params.data)

	field, ok := q.params.fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.GetValue(val), nil
}

type termsQuery struct {
	params queryParams
}

func newTermsQuery(params queryParams) (*termsQuery, error) {
	if len(params.data) == 0 {
		return nil, NewErrSyntax(errMsgCantBeEmpty(), params.path)
	}
	if len(params.data) > 1 {
		return nil, NewErrSyntax(errMsgCantHaveMultipleFields(), params.path)
	}

	return &termsQuery{
		params: params,
	}, nil
}

func (q *termsQuery) exec() (*roaring.Bitmap, error) {
	key, val := firstVal(q.params.data)

	values, err := interfaceToSlice[interface{}](val)
	if err != nil {
		return nil, NewErrSyntax(errMsgArrayValueRequired(), pathJoin(q.params.path, key))
	}

	field, ok := q.params.fields[key]
	if !ok {
		return roaring.New(), nil
	}

	return field.GetValuesOr(values), nil
}
