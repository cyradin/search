package index

import (
	"testing"

	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_Documents_Add(t *testing.T) {
	data := []struct {
		name      string
		source    DocSource
		erroneous bool
		expected  uint32
	}{
		{
			name:      "field_value_set_err",
			source:    DocSource{"v": "1"},
			erroneous: true,
			expected:  1,
		},
		{
			name:      "ok",
			source:    DocSource{"v": true},
			erroneous: false,
			expected:  1,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			i := New(
				"name",
				schema.New(
					map[string]schema.Field{"v": schema.NewField(schema.TypeBool, true, "")},
					nil,
				),
			)

			docs := NewDocuments(t.TempDir())

			err := docs.AddIndex(i)
			require.NoError(t, err)

			guid, err := docs.Add(i, "", d.source)
			if d.erroneous {
				require.Error(t, err)
				require.NotEmpty(t, guid)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, guid)
			}
		})
	}
}
