package analyzer

import (
	"fmt"
)

type Type string
type Analyzer struct {
	Type     Type
	Settings map[string]interface{}
}

func New(t Type, settings map[string]interface{}) Analyzer {
	return Analyzer{
		Type:     t,
		Settings: settings,
	}
}

type Func func([]string) []string
type Handler func(next Func) Func

const (
	Nop                 Type = "nop"
	Dedup               Type = "dedup"
	TokenizerWhitespace Type = "whitespace"
	TokenizerRegexp     Type = "regexp"
)

// GetFunc get analyzer func by name
func GetFunc(a Analyzer) (Func, error) {
	switch a.Type {
	case Nop:
		return NopFunc(), nil
	case Dedup:
		return DedupFunc(), nil
	case TokenizerWhitespace:
		return TokenizerWhitespaceFunc(), nil
	case TokenizerRegexp:
		return TokenizerRegexpFunc(a.Settings)
	}

	return nil, fmt.Errorf("unknown type %q", a.Type)
}

// Chain build analyzer chain by their names
func Chain(types []Analyzer) (Func, error) {
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
