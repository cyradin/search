package entity

type Search struct {
	Query  Query `json:"query"`
	Limit  int   `json:"limit"`
	Offset int   `json:"offset"`
}

type Query map[string]interface{}
