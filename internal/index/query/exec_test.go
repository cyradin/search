package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/stretchr/testify/require"
)

func Test_Exec(t *testing.T) {
	t.Run("must return matching doc ids", func(t *testing.T) {
		f, err := field.New(schema.TypeBool)
		require.NoError(t, err)
		f.Add(1, true)

		query := []byte(`{
			"type": "term",
			"field": "field",
			"query": 1
		}`)

		result, err := Exec(context.Background(), query, map[string]field.Field{"field": f})
		require.NoError(t, err)
		require.EqualValues(t, Result{Hits: []Hit{{ID: 1, Score: 1}}, Total: Total{Value: 1, Relation: "eq"}, MaxScore: 1}, result)
	})
}
