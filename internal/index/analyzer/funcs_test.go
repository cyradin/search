package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_nopFunc(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var data []string
		result := nopFunc(data)
		require.Equal(t, data, result)
	})
	t.Run("not empty", func(t *testing.T) {
		data := []string{"qwerty", "asdfgh"}
		result := nopFunc(data)
		require.Equal(t, data, result)
	})
}
