package analyzer

import "strings"

// NopFunc Does nothing
func NopFunc(next Func) Func {
	return func(s []string) []string {
		return next(s)
	}
}

// WhitespaceTokenizerFunc splits string by whitespace characters (see strings.Fields)
func WhitespaceTokenizerFunc(next Func) Func {
	return func(s []string) []string {
		if len(s) == 0 {
			return s
		}

		result := make([]string, 0, len(s))

		for _, str := range s {
			result = append(result, strings.Fields(str)...)
		}

		return result
	}
}

// DedupFunc leaves only the first copy of the token
func DedupFunc(next Func) Func {
	return func(s []string) []string {
		if len(s) == 0 || len(s) == 1 {
			return s
		}

		result := make([]string, 0, len(s))
		m := make(map[string]struct{})
		for _, str := range s {
			if _, ok := m[str]; ok {
				continue
			}
			m[str] = struct{}{}
			result = append(result, str)
		}

		return result
	}
}
