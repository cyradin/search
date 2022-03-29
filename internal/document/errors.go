package document

import "fmt"

var _ error = (*ErrNotFound)(nil)
var _ error = (*ErrAlreadyExists)(nil)

type ErrNotFound struct {
	id string
}

func NewErrNotFound(id string) *ErrNotFound {
	return &ErrNotFound{id: id}
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("document #%s not found", e.id)
}

type ErrAlreadyExists struct {
	id string
}

func NewErrAlreadyExists(id string) *ErrAlreadyExists {
	return &ErrAlreadyExists{id: id}
}

func (e *ErrAlreadyExists) Error() string {
	return fmt.Sprintf("document #%s already exists", e.id)
}
