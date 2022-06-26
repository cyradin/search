package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GetFunc(t *testing.T) {
	t.Run("can get func by valid analyzer name", func(t *testing.T) {
		f, err := GetFunc(Nop)
		require.NoError(t, err)
		require.NotNil(t, f)
	})

	t.Run("cannot get func by invalid analyzer name", func(t *testing.T) {
		f, err := GetFunc("")
		require.Error(t, err)
		require.Nil(t, f)
	})
}

func Test_Chain(t *testing.T) {
	t.Run("cannot build chain if empty slice provided", func(t *testing.T) {
		f, err := Chain(nil)
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("cannot build chain if invalid analyzer name provided", func(t *testing.T) {
		f, err := Chain([]Type{Nop, ""})
		require.Error(t, err)
		require.Nil(t, f)
	})

	t.Run("can build chain by valid analyzer names", func(t *testing.T) {
		f, err := Chain([]Type{Whitespace, Dedup})

		require.NoError(t, err)
		require.NotNil(t, f)

		result := f([]string{"hello world", "hello", "world"})
		require.Equal(t, []string{"hello", "world"}, result)
	})
}
