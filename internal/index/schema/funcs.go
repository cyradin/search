package schema

// NopFunc Does nothing
func NopFunc() AnalyzerFunc {
	return func(s []string) []string {
		return s
	}
}

// DedupFunc leaves only the first copy of the token
func DedupFunc() AnalyzerFunc {
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
