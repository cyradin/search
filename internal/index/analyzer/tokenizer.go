package analyzer

import (
	"regexp"
	"strings"
)

// TokenizerWhitespaceFunc splits string by whitespace characters (see strings.Fields)
func TokenizerWhitespaceFunc() Func {
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

// TokenizerRegexpFunc splits string by regular expression
func TokenizerRegexpFunc(expression string) (Func, error) {
	exp, err := regexp.Compile(expression)
	if err != nil {
		return nil, err
	}

	return func(s []string) []string {
		if len(s) == 0 {
			return s
		}

		result := make([]string, 0, len(s))

		for _, str := range s {
			for _, part := range exp.Split(str, -1) {
				if part == "" {
					continue
				}
				result = append(result, part)
			}
		}

		return result
	}, nil
}
