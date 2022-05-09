package index

import "fmt"

var _ error = (*ErrNotFound)(nil)
var _ error = (*ErrAlreadyExists)(nil)

type ErrNotFound struct {
	id string
}

func NewErrDocNotFound(id string) *ErrNotFound {
	return &ErrNotFound{id: id}
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("document #%s not found", e.id)
}

type ErrAlreadyExists struct {
	id string
}

func NewErrDocAlreadyExists(id string) *ErrAlreadyExists {
	return &ErrAlreadyExists{id: id}
}

func (e *ErrAlreadyExists) Error() string {
	return fmt.Sprintf("document #%s already exists", e.id)
}

type ErrEmptyId struct{}

func NewErrEmptyDocId() *ErrEmptyId { return &ErrEmptyId{} }

func (e *ErrEmptyId) Error() string {
	return "doc id must be defined"
}
