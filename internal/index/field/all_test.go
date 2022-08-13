package field

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

type testFieldValue struct {
	id    uint32
	value interface{}
}

func Test_All_Add(t *testing.T) {
	field := newAll()
	field.Add(1, true)

	require.True(t, field.data.Contains(1))
	require.False(t, field.data.Contains(2))
}

func Test_All_TermQuery(t *testing.T) {
	field := newAll()
	field.Add(1, true)

	result := field.TermQuery(context.Background(), true)
	require.True(t, result.Docs().Contains(1))
}

func Test_All_MatchQuery(t *testing.T) {
	field := newAll()
	field.Add(1, true)

	result := field.MatchQuery(context.Background(), true)
	require.True(t, result.Docs().Contains(1))
}

func Test_All_Delete(t *testing.T) {
	t.Run("can delete value", func(t *testing.T) {
		field := newAll()
		field.Add(1, true)

		field.Delete(1)
		require.EqualValues(t, 0, field.data.GetCardinality())
	})

	t.Run("cannot delete other values", func(t *testing.T) {
		field := newAll()
		field.Add(1, true)
		field.Add(2, true)

		field.Delete(1)
		require.EqualValues(t, 1, field.data.GetCardinality())
		require.True(t, field.data.Contains(2))
	})
}

func Test_All_TermAgg(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	field := newAll()
	field.Add(1, true)
	result := field.TermAgg(context.Background(), bm, 20)
	require.Equal(t, []TermBucket{}, result.Buckets)
}

func Test_All_Marshal(t *testing.T) {
	field := newAll()
	field.Add(1, true)

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := newAll()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.data.Contains(1))
}
