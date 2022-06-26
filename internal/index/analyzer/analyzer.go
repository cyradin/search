package analyzer

import (
	"fmt"
)

type Type string

type Func func([]string) []string
type Handler func(next Func) Func

const (
	Nop        Type = "nop"
	Dedup      Type = "dedup"
	Whitespace Type = "whitespace"
)

// Valid check if analyzer is valid
func Valid(t Type) bool {
	return t == Nop
}

// GetFunc get analyzer func by name
func GetFunc(t Type) (Func, error) {
	switch t {
	case Nop:
		return NopFunc(), nil
	case Dedup:
		return DedupFunc(), nil
	case Whitespace:
		return WhitespaceFunc(), nil
	}

	return nil, fmt.Errorf("unknown type %q", t)
}

// Chain build analyzer chain by their names
func Chain(types []Type) (Func, error) {
	if len(types) == 0 {
		return nil, fmt.Errorf("chain cannot be empty")
	}

	var h Func
	for i := len(types) - 1; i >= 0; i-- {
		f, err := GetFunc(types[i])
		if err != nil {
			return nil, err
		}

		h = handler(f, h)
	}

	return h, nil
}

func handler(current Func, next Func) Func {
	if next == nil {
		return current
	}

	return func(s []string) []string {
		return next(current(s))
	}
}
