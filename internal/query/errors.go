package query

import (
	"fmt"
	"strings"
)

type ErrSyntax struct {
	msg  string
	path string
}

func NewErrSyntax(msg string, path string) *ErrSyntax {
	return &ErrSyntax{
		msg:  msg,
		path: path,
	}
}

func (e *ErrSyntax) Error() string {
	return fmt.Sprintf("syntax err: path %q: %s", e.path, e.msg)
}

func errMsgCantBeEmpty() string {
	return "cannot be empty"
}

func errMsgCantHaveMultipleFields() string {
	return "cannot have multiple fields"
}

func errMsgArrayValueRequired() string {
	return "value must be an array"
}

func errMsgObjectValueRequired() string {
	return "value must be an object"
}

func errMsgOneOf(values []string, invalid string) string {
	return fmt.Sprintf("provided: %q, but required one of: %s", invalid, strings.Join(values, ","))
}
