package query

type Query struct {
	Bool  *Bool  `json:"bool"`
	Term  *Term  `json:"term"`
	Terms *Terms `json:"terms"`
}

type Bool struct {
	Should []Query `json:"should"`
	Must   []Query `json:"must"`
	Filter []Query `json:"filter"`
}

type Term struct {
	Field string      `json:"field" validate:"required"`
	Value interface{} `json:"value" validate:"required"`
}

type Terms struct {
	Field  string      `json:"field" validate:"required"`
	Values interface{} `json:"values" validate:"required"`
}
