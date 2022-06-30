package schema

import (
	"context"

	"github.com/cyradin/search/internal/index/analyzer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type FieldAnalyzer struct {
	Analyzers []Analyzer `json:"analyzers"`
}

func (fa FieldAnalyzer) Build() (analyzer.Func, error) {
	items := make([]analyzer.Analyzer, len(fa.Analyzers))
	for i, a := range fa.Analyzers {
		items[i] = analyzer.New(analyzer.Type(a.Type), a.Settings)
	}

	return analyzer.Chain(items)
}

func (a FieldAnalyzer) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.Analyzers, validation.Required, validation.Length(1, 0)),
	)
}

type Analyzer struct {
	Type     analyzer.Type          `json:"type"`
	Settings map[string]interface{} `json:"settings"`
}

func (a Analyzer) Validate() error {
	_, err := analyzer.GetFunc(analyzer.Analyzer{
		Type:     analyzer.Type(a.Type),
		Settings: a.Settings,
	})
	return err
}

type Schema struct {
	Analyzers map[string]FieldAnalyzer `json:"analyzers"`
	Fields    map[string]Field         `json:"fields"`
}

func New(fields map[string]Field, analyzers map[string]FieldAnalyzer) Schema {
	return Schema{
		Fields:    fields,
		Analyzers: analyzers,
	}
}

func (s Schema) ValidateDoc(doc map[string]interface{}) error {
	return ValidateDoc(s, doc)
}

func (s Schema) Validate() error {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "schema", s)

	return validation.ValidateStructWithContext(ctx, &s,
		validation.Field(&s.Fields, validation.Required),
		validation.Field(&s.Analyzers),
	)
}
