package analyzer

type Type string
type Func func([]string) []string

const (
	Nop Type = "nop"
)

// Valid check if analyzer is valid
func Valid(t Type) bool {
	return t == Nop
}
