package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/document"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_Index_Add(t *testing.T) {
	data := []struct {
		name         string
		guid         string
		generator    func() string
		sourceInsert sourceInserter
		source       map[string]interface{}
		erroneous    bool
		expected     string
	}{
		{
			name: "empty_id",
			guid: "",
			generator: func() string {
				return "id"
			},
			source: map[string]interface{}{"v": true},
			sourceInsert: func(guid string, doc *document.Document) error {
				return nil
			},
			erroneous: false,
			expected:  "id",
		},
		{
			name: "source_insert_err",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: map[string]interface{}{"v": true},
			sourceInsert: func(guid string, doc *document.Document) error {
				return fmt.Errorf("err")
			},
			erroneous: true,
			expected:  "id",
		},
		{
			name: "field_value_set_err",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: map[string]interface{}{"v": "1"},
			sourceInsert: func(guid string, doc *document.Document) error {
				return nil
			},
			erroneous: true,
			expected:  "id",
		},
		{
			name: "ok",
			guid: "id",
			generator: func() string {
				return "id"
			},
			source: map[string]interface{}{"v": true},
			sourceInsert: func(guid string, doc *document.Document) error {
				return nil
			},
			erroneous: false,
			expected:  "id",
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()

			index := &Index{
				idSet:        func(uid string) uint32 { return 1 },
				sourceInsert: d.sourceInsert,
				fields: map[string]field.Field{
					"v": field.NewBool(ctx),
				},
				guidGenerate: d.generator,
				schema:       &schema.Schema{},
			}
			guid, err := index.Add(d.guid, d.source)
			if d.erroneous {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, d.expected, guid)
			}
		})
	}
}
