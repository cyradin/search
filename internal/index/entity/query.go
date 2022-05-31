package entity

type Search struct {
	Query  map[string]interface{} `json:"query"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

type SearchResult struct {
}
