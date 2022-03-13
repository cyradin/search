package document

import "fmt"

var _ error = (*ErrNotFound)(nil)

type ErrNotFound struct {
	id string
}

func NewErrNotFound(id string) *ErrNotFound {
	return &ErrNotFound{id: id}
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("document #%s not found", e.id)
}
