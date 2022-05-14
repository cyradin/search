package query

import "fmt"

type ErrSyntax struct {
	msg string
}

func NewErrSyntax(msg string) *ErrSyntax {
	return &ErrSyntax{
		msg: msg,
	}
}

func (e *ErrSyntax) Error() string {
	return fmt.Sprintf("syntax err: %s", e.msg)
}
