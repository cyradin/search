package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_Exec(t *testing.T) {
	t.Run("must return matching doc ids", func(t *testing.T) {
		f := field.NewBool()
		f.Add(1, true)

		req, err := decodeQuery(`{
			"term": {
				"field": {
					"query": 1
				}
			}
		}`)
		require.NoError(t, err)
		require.NoError(t, err)

		result, err := Exec(context.Background(), req, map[string]field.Field{"field": f})
		require.NoError(t, err)
		require.EqualValues(t, Result{Hits: []Hit{{ID: 1, Score: 1}}, Total: Total{Value: 1, Relation: "eq"}, MaxScore: 1}, result)
	})
}
