package field

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

func Test_Keyword_Add(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		field := newKeyword()
		field.Add(1, true)

		require.EqualValues(t, 1, field.values.DocsByValue("true").GetCardinality())
		require.True(t, field.values.DocsByValue("true").Contains(1))
	})
	t.Run("string", func(t *testing.T) {
		field := newKeyword()
		field.Add(1, "foo")
		field.Add(1, "bar")
		field.Add(2, "foo")

		require.EqualValues(t, 2, field.values.DocsByValue("foo").GetCardinality())
		require.EqualValues(t, 1, field.values.DocsByValue("bar").GetCardinality())
		require.True(t, field.values.DocsByValue("foo").Contains(1))
		require.True(t, field.values.DocsByValue("foo").Contains(2))
		require.True(t, field.values.DocsByValue("bar").Contains(1))
		require.False(t, field.values.DocsByValue("bar").Contains(2))
	})
}

func Test_Keyword_TermQuery(t *testing.T) {
	field := newKeyword()
	field.Add(1, "foo")

	result := field.TermQuery(context.Background(), "foo")
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.TermQuery(context.Background(), "bar")
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_Keyword_MatchQuery(t *testing.T) {
	field := newKeyword()
	field.Add(1, "foo")

	result := field.MatchQuery(context.Background(), "foo")
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.MatchQuery(context.Background(), "bar")
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_Keyword_Delete(t *testing.T) {
	field := newKeyword()
	field.Add(1, "foo")
	field.Add(1, "bar")
	field.Add(2, "foo")

	field.Delete(2)
	require.EqualValues(t, 1, field.values.DocsByValue("foo").GetCardinality())
	require.EqualValues(t, 1, field.values.DocsByValue("bar").GetCardinality())
	require.ElementsMatch(t, []string{"foo", "bar"}, field.values.ValuesByDoc(1))
	require.Empty(t, field.values.ValuesByDoc(2))

	field.Delete(1)
	require.Empty(t, field.values.DocsByValue("foo").ToArray())
	require.Empty(t, field.values.DocsByValue("bar").ToArray())
	require.Empty(t, field.values.ValuesByDoc(1))
}

func Test_Keyword_Data(t *testing.T) {
	field := newKeyword()
	field.Add(1, "foo")
	field.Add(1, "bar")
	field.Add(2, "foo")

	result := field.Data(1)
	require.ElementsMatch(t, []string{"foo", "bar"}, result)

	result = field.Data(2)
	require.ElementsMatch(t, []string{"foo"}, result)
}

func Test_Keyword_TermAgg(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	field := newKeyword()
	field.Add(1, "foo")
	result := field.TermAgg(context.Background(), bm, 20)
	require.Equal(t, []TermBucket{
		{Key: "foo", Docs: bm},
	}, result.Buckets)
}

func Test_Keyword_RangeAgg(t *testing.T) {
	bm := roaring.New()
	bm.Add(1)

	field := newKeyword()
	field.Add(1, true)
	result := field.RangeAgg(context.Background(), bm, []Range{{From: 1, To: 2, Key: "key"}})
	require.Equal(t, []RangeBucket{{From: 1, To: 2, Key: "key", Docs: roaring.New()}}, result.Buckets)
}

func Test_Keyword_Marshal(t *testing.T) {
	field := newKeyword()
	field.Add(1, "foo")
	field.Add(1, "bar")
	field.Add(2, "foo")

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := newKeyword()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.values.DocsByValue("foo").Contains(1))
	require.True(t, field2.values.DocsByValue("bar").Contains(1))
	require.ElementsMatch(t, []string{"foo", "bar"}, field.values.ValuesByDoc(1))
	require.True(t, field2.values.DocsByValue("foo").Contains(2))
	require.ElementsMatch(t, []string{"foo"}, field.values.ValuesByDoc(2))
}
