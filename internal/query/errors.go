package query

import "fmt"

type syntaxErrType string

const (
	cannotBeEmpty            syntaxErrType = "cannot-be-empty"
	cannotHaveMultipleFields syntaxErrType = "cannot-have-multiple-fields"
	arrayValueRequired       syntaxErrType = "array-value-required"
)

type ErrSyntax struct {
	errType syntaxErrType
	field   string
}

func NewErrSyntax(t syntaxErrType, field string) *ErrSyntax {
	return &ErrSyntax{
		errType: t,
		field:   field,
	}
}

func (e *ErrSyntax) Error() string {
	var msg string

	switch e.errType {
	case cannotBeEmpty:
		msg = "cannot be empty"
	case cannotHaveMultipleFields:
		msg = "cannot have multiple fields"
	case arrayValueRequired:
		msg = "values must be an array"
	}

	return fmt.Sprintf("syntax err: field %s: %s", e.field, msg)
}
