package schema

import (
	"testing"

	"github.com/cyradin/search/internal/index/analyzer"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_Schema_Validate(t *testing.T) {
	t.Run("must fail if field name is empty", func(t *testing.T) {
		s := New(map[string]Field{
			"": {Type: TypeKeyword},
		}, nil)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is empty", func(t *testing.T) {
		s := New(map[string]Field{
			"name": {Name: "name"},
		}, nil)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is invalid", func(t *testing.T) {
		s := New(map[string]Field{
			"name": {Name: "name", Type: "invalid"},
		}, nil)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type cannot have child types", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeBool, Children: map[string]Field{
					"name": {Name: "name"},
				}},
			},
			nil,
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type must have children but there aren't any", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeSlice},
			},
			nil,
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field child validation fails", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeSlice, Children: map[string]Field{
					"": {Name: "", Type: TypeBool},
				}},
			}, nil,
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has no analyzers", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText},
			},
			nil,
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has unknown analyzer", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText, Analyzer: "invalid"},
			},
			nil,
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if analyzer has invalid type", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText, Analyzer: "analyzer"},
			},
			map[string]FieldAnalyzer{
				"analyzer": {
					Analyzers: []Analyzer{
						{Type: "invalid", Settings: nil},
					},
				},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if analyzer has invalid settings", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name": {Name: "name", Type: TypeText, Analyzer: "analyzer"},
			},
			map[string]FieldAnalyzer{
				"analyzer": {
					Analyzers: []Analyzer{
						{Type: analyzer.TokenizerRegexp, Settings: nil},
					},
				},
			},
		)
		err := validation.Validate(s)
		require.Error(t, err)
	})

	t.Run("must not fail for vaild fields", func(t *testing.T) {
		s := New(
			map[string]Field{
				"name":  {Name: "name", Type: TypeBool},
				"name2": {Name: "name2", Type: TypeText, Analyzer: "analyzer"},
				"name3": {Name: "name3", Type: TypeSlice, Children: map[string]Field{
					"name": {Name: "name", Type: TypeKeyword},
				}},
			},
			map[string]FieldAnalyzer{
				"analyzer": {Analyzers: []Analyzer{
					{Type: analyzer.TokenizerRegexp, Settings: map[string]interface{}{"pattern": "\\s"}},
				}},
			},
		)
		err := validation.Validate(s)
		require.NoError(t, err)
	})
}
