package schema

import "github.com/cyradin/search/internal/errs"

type AnalyzerFunc func([]string) []string
type AnalyzerHandler func(next AnalyzerFunc) AnalyzerFunc
type AnalyzerType string

type Analyzer struct {
	Type     AnalyzerType           `json:"type"`
	Settings map[string]interface{} `json:"settings"`
}

func NewAnalyzer(t AnalyzerType, settings map[string]interface{}) Analyzer {
	return Analyzer{
		Type:     t,
		Settings: settings,
	}
}

func (a Analyzer) Validate() error {
	_, err := a.GetFunc()
	return err
}

// GetFunc get analyzer func by name
func (a Analyzer) GetFunc() (AnalyzerFunc, error) {
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

	return nil, errs.Errorf("unknown type %q", a.Type)
}

const (
	Nop                 AnalyzerType = "nop"
	Dedup               AnalyzerType = "dedup"
	TokenizerWhitespace AnalyzerType = "whitespace"
	TokenizerRegexp     AnalyzerType = "regexp"
)

// Chain build analyzer chain by their names
func Chain(items []Analyzer) (AnalyzerFunc, error) {
	if len(items) == 0 {
		return nil, errs.Errorf("chain cannot be empty")
	}

	var h AnalyzerFunc
	for i := len(items) - 1; i >= 0; i-- {
		f, err := items[i].GetFunc()
		if err != nil {
			return nil, err
		}

		h = handler(f, h)
	}

	return h, nil
}

func handler(current AnalyzerFunc, next AnalyzerFunc) AnalyzerFunc {
	if next == nil {
		return current
	}

	return func(s []string) []string {
		return next(current(s))
	}
}
