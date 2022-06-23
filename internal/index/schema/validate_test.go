package schema

import (
	"testing"

	"github.com/cyradin/search/internal/index/analyzer"
	"github.com/stretchr/testify/require"
)

func Test_Validate(t *testing.T) {
	t.Run("must fail if field name is empty", func(t *testing.T) {
		s := New([]Field{
			{Type: TypeKeyword},
		})
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if two fields has the same name", func(t *testing.T) {
		s := New([]Field{
			{Type: TypeKeyword, Name: "name"},
			{Type: TypeKeyword, Name: "name"},
		})
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is empty", func(t *testing.T) {
		s := New([]Field{
			{Name: "name"},
		})
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type is invalid", func(t *testing.T) {
		s := New([]Field{
			{Name: "name", Type: "invalid"},
		})
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type cannot have child types", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeBool, Children: []Field{
					{Name: "name"},
				}},
			},
		)
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field type must have children but there aren't any", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeSlice},
			},
		)
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if field child validation fails", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeSlice, Children: []Field{
					{Name: "", Type: TypeBool},
				}},
			},
		)
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has no analyzers", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeText},
			},
		)
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must fail if text field has invalid analyzer", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeText, Analyzers: []analyzer.Type{"invalid"}},
			},
		)
		err := Validate(s)
		require.Error(t, err)
	})

	t.Run("must not fail for vaild fields", func(t *testing.T) {
		s := New(
			[]Field{
				{Name: "name", Type: TypeBool},
				{Name: "name2", Type: TypeText, Analyzers: []analyzer.Type{analyzer.Nop}},
				{Name: "name3", Type: TypeSlice, Children: []Field{
					{Name: "name", Type: TypeKeyword},
				}},
			},
		)
		err := Validate(s)
		require.NoError(t, err)
	})
}
