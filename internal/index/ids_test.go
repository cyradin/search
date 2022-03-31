package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IDs_Get(t *testing.T) {
	data := []struct {
		name     string
		uid      string
		ids      docIDs
		expected uint32
	}{
		{
			name:     "empty",
			uid:      "qwerty",
			ids:      docIDs{},
			expected: 0,
		},
		{
			name:     "not_exists",
			uid:      "asdfgh",
			ids:      docIDs{"qwerty": 1},
			expected: 0,
		},
		{
			name:     "exists",
			uid:      "qwerty",
			ids:      docIDs{"qwerty": 1},
			expected: 1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ids := NewIDs(0, d.ids)
			id := ids.Get(d.uid)
			assert.Equal(t, d.expected, id)
		})
	}
}

func Test_IDs_Set(t *testing.T) {
	data := []struct {
		name     string
		max      uint32
		uid      string
		ids      docIDs
		expected uint32
	}{
		{
			name:     "empty",
			uid:      "qwerty",
			ids:      docIDs{},
			expected: 1,
		},
		{
			name:     "not_exists",
			uid:      "asdfgh",
			ids:      docIDs{"qwerty": 1},
			max:      1,
			expected: 2,
		},
		{
			name:     "exists",
			uid:      "qwerty",
			ids:      docIDs{"qwerty": 1},
			max:      1,
			expected: 1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ids := NewIDs(d.max, d.ids)
			id := ids.Set(d.uid)
			assert.Equal(t, d.expected, id)
		})
	}
}
