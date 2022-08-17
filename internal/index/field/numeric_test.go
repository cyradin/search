package field

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

func Test_Numeric_Add(t *testing.T) {
	t.Run("intumeric8", test_Numeric_Add[int8])
	t.Run("int16", test_Numeric_Add[int16])
	t.Run("int32", test_Numeric_Add[int32])
	t.Run("int64", test_Numeric_Add[int64])
	t.Run("uint64", test_Numeric_Add[uint64])
	t.Run("float32", test_Numeric_Add[float32])
	t.Run("float64", test_Numeric_Add[float64])
}

func test_Numeric_Add[T NumericConstraint](t *testing.T) {
	t.Run("string", func(t *testing.T) {
		field := newNumeric[T]()
		field.Add(1, "qwe")

		require.EqualValues(t, 0, len(field.values.Docs))
	})
	t.Run("numeric", func(t *testing.T) {
		field := newNumeric[T]()
		field.Add(1, 10)
		field.Add(1, 20)
		field.Add(1, 50)
		field.Add(1, 1)
		field.Add(1, 20)

		field.Add(2, 10)

		require.EqualValues(t, 1, field.values.DocsByValue(1).GetCardinality())
		require.EqualValues(t, 2, field.values.DocsByValue(10).GetCardinality())
		require.EqualValues(t, 1, field.values.DocsByValue(20).GetCardinality())
		require.EqualValues(t, 1, field.values.DocsByValue(50).GetCardinality())
		require.True(t, field.values.DocsByValue(1).Contains(1))
		require.True(t, field.values.DocsByValue(10).Contains(1))
		require.True(t, field.values.DocsByValue(20).Contains(1))
		require.True(t, field.values.DocsByValue(50).Contains(1))

		require.True(t, field.values.DocsByValue(10).Contains(2))
		require.False(t, field.values.DocsByValue(1).Contains(2))

		require.ElementsMatch(t, []T{1, 10, 20, 50}, field.values.ValuesByDoc(1))
		require.ElementsMatch(t, []T{10}, field.values.ValuesByDoc(2))
	})
}

func Test_Numeric_TermQuery(t *testing.T) {
	t.Run("int8", test_Numeric_TermQuery[int8])
	t.Run("int16", test_Numeric_TermQuery[int16])
	t.Run("int32", test_Numeric_TermQuery[int32])
	t.Run("int64", test_Numeric_TermQuery[int64])
	t.Run("uint64", test_Numeric_TermQuery[uint64])
	t.Run("float32", test_Numeric_TermQuery[float32])
	t.Run("float64", test_Numeric_TermQuery[float64])
}

func test_Numeric_TermQuery[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)

	result := field.TermQuery(context.Background(), 1)
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.TermQuery(context.Background(), 2)
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_Numeric_MatchQuery(t *testing.T) {
	t.Run("int8", test_Numeric_TermQuery[int8])
	t.Run("int16", test_Numeric_TermQuery[int16])
	t.Run("int32", test_Numeric_TermQuery[int32])
	t.Run("int64", test_Numeric_TermQuery[int64])
	t.Run("uint64", test_Numeric_TermQuery[uint64])
	t.Run("float32", test_Numeric_TermQuery[float32])
	t.Run("float64", test_Numeric_TermQuery[float64])
}

func test_Numeric_MatchQuery[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)

	result := field.MatchQuery(context.Background(), 1)
	require.True(t, result.Docs().Contains(1))
	require.EqualValues(t, 1, result.Docs().GetCardinality())

	result = field.MatchQuery(context.Background(), 2)
	require.False(t, result.Docs().Contains(1))
	require.EqualValues(t, 0, result.Docs().GetCardinality())
}

func Test_Numeric_RangeQuery(t *testing.T) {
	t.Run("int8", test_Numeric_RangeQuery[int8])
	t.Run("int16", test_Numeric_RangeQuery[int16])
	t.Run("int32", test_Numeric_RangeQuery[int32])
	t.Run("int64", test_Numeric_RangeQuery[int64])
	t.Run("uint64", test_Numeric_RangeQuery[uint64])
	t.Run("float32", test_Numeric_RangeQuery[float32])
	t.Run("float64", test_Numeric_RangeQuery[float64])
}

func test_Numeric_RangeQuery[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)
	field.Add(2, 2)
	field.Add(3, 3)
	field.Add(4, 4)
	field.Add(5, 5)
	field.Add(6, 6)

	t.Run("no values", func(t *testing.T) {
		result := field.RangeQuery(context.Background(), nil, nil, false, false)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("(1..", func(t *testing.T) {
		var from int32 = 1
		result := field.RangeQuery(context.Background(), &from, nil, false, false)
		require.ElementsMatch(t, []uint32{2, 3, 4, 5, 6}, result.Docs().ToArray())
	})
	t.Run("[1..", func(t *testing.T) {
		var from int32 = 1
		result := field.RangeQuery(context.Background(), &from, nil, true, false)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.Docs().ToArray())
	})
	t.Run("..3)", func(t *testing.T) {
		var to int32 = 3
		result := field.RangeQuery(context.Background(), nil, &to, false, false)
		require.ElementsMatch(t, []uint32{1, 2}, result.Docs().ToArray())
	})
	t.Run("..3]", func(t *testing.T) {
		var to int32 = 3
		result := field.RangeQuery(context.Background(), nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
	})
	t.Run("..6]", func(t *testing.T) {
		var to int32 = 6
		result := field.RangeQuery(context.Background(), nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.Docs().ToArray())
	})
	t.Run("..7]", func(t *testing.T) {
		var to int32 = 6
		result := field.RangeQuery(context.Background(), nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.Docs().ToArray())
	})
	t.Run("(1..4]", func(t *testing.T) {
		var from int32 = 1
		var to int32 = 4
		result := field.RangeQuery(context.Background(), &from, &to, false, true)
		require.ElementsMatch(t, []uint32{2, 3, 4}, result.Docs().ToArray())
	})
	t.Run("[100, 1000]", func(t *testing.T) {
		var from int32 = 100
		var to int32 = 1000
		result := field.RangeQuery(context.Background(), &from, &to, false, true)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
}

func Test_Numeric_DeleteDoc(t *testing.T) {
	t.Run("int8", test_Numeric_Delete[int8])
	t.Run("int16", test_Numeric_Delete[int16])
	t.Run("int32", test_Numeric_Delete[int32])
	t.Run("int64", test_Numeric_Delete[int64])
	t.Run("uint64", test_Numeric_Delete[uint64])
	t.Run("float32", test_Numeric_Delete[float32])
	t.Run("float64", test_Numeric_Delete[float64])
}

func test_Numeric_Delete[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 10)
	field.Add(1, 20)
	field.Add(1, 50)
	field.Add(1, 2)
	field.Add(1, 20)
	field.Add(2, 1)
	field.Add(2, 10)
	field.Add(2, 30)
	field.Add(2, 60)

	field.DeleteDoc(2)
	require.EqualValues(t, 1, field.values.DocsByValue(2).GetCardinality())
	require.EqualValues(t, 1, field.values.DocsByValue(10).GetCardinality())
	require.EqualValues(t, 1, field.values.DocsByValue(20).GetCardinality())
	require.EqualValues(t, 1, field.values.DocsByValue(50).GetCardinality())
	require.Empty(t, field.values.DocsByValue(1).GetCardinality())
	require.Empty(t, field.values.DocsByValue(30).GetCardinality())
	require.Empty(t, field.values.DocsByValue(60).GetCardinality())
	require.ElementsMatch(t, []T{2, 10, 20, 50}, field.values.ValuesByDoc(1))
	require.Empty(t, field.values.ValuesByDoc(2))

	field.DeleteDoc(1)
	require.Empty(t, field.values.DocsByValue(1).GetCardinality())
	require.Empty(t, field.values.DocsByValue(10).GetCardinality())
	require.Empty(t, field.values.DocsByValue(20).GetCardinality())
	require.Empty(t, field.values.DocsByValue(50).GetCardinality())
	require.Empty(t, field.values.ValuesByDoc(1))
}

func Test_Numeric_Data(t *testing.T) {
	t.Run("int8", test_Numeric_Data[int8])
	t.Run("int16", test_Numeric_Data[int16])
	t.Run("int32", test_Numeric_Data[int32])
	t.Run("int64", test_Numeric_Data[int64])
	t.Run("uint64", test_Numeric_Data[uint64])
	t.Run("float32", test_Numeric_Data[float32])
	t.Run("float64", test_Numeric_Data[float64])
}

func test_Numeric_Data[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)
	field.Add(1, 2)
	field.Add(2, 1)

	result := field.Data(1)
	require.ElementsMatch(t, []T{T(1), T(2)}, result)
	result = field.Data(2)
	require.ElementsMatch(t, []T{T(1)}, result)
}

func Test_Numeric_TermAgg(t *testing.T) {
	t.Run("int8", test_Numeric_TermAgg[int8])
	t.Run("int16", test_Numeric_TermAgg[int16])
	t.Run("int32", test_Numeric_TermAgg[int32])
	t.Run("int64", test_Numeric_TermAgg[int64])
	t.Run("uint64", test_Numeric_TermAgg[uint64])
	t.Run("float32", test_Numeric_TermAgg[float32])
	t.Run("float64", test_Numeric_TermAgg[float64])
}

func test_Numeric_TermAgg[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)
	field.Add(1, 2)
	field.Add(2, 1)

	bm1 := roaring.New()
	bm1.Add(1)
	bm1.Add(2)

	bm2 := roaring.New()
	bm2.Add(1)

	result := field.TermAgg(context.Background(), bm1, 20)
	require.EqualValues(t, []TermBucket{
		{Key: T(1), Docs: bm1},
		{Key: T(2), Docs: bm2},
	}, result.Buckets)
}

func Test_Numeric_Marshal(t *testing.T) {
	t.Run("int8", test_Numeric_Marshal[int8])
	t.Run("int16", test_Numeric_Marshal[int16])
	t.Run("int32", test_Numeric_Marshal[int32])
	t.Run("int64", test_Numeric_Marshal[int64])
	t.Run("uint64", test_Numeric_Marshal[uint64])
	t.Run("float32", test_Numeric_Marshal[float32])
	t.Run("float64", test_Numeric_Marshal[float64])
}

func test_Numeric_Marshal[T NumericConstraint](t *testing.T) {
	field := newNumeric[T]()
	field.Add(1, 1)
	field.Add(1, 2)
	field.Add(2, 1)

	data, err := field.MarshalBinary()
	require.NoError(t, err)

	field2 := newNumeric[T]()
	err = field2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.True(t, field2.values.DocsByValue(1).Contains(1))
	require.True(t, field2.values.DocsByValue(2).Contains(1))
	require.ElementsMatch(t, []T{1, 2}, field.values.ValuesByDoc(1))
	require.True(t, field2.values.DocsByValue(1).Contains(2))
	require.ElementsMatch(t, []T{1}, field.values.ValuesByDoc(2))
}

func Test_rangeQuery(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	v.Add(2, 2)
	v.Add(3, 3)
	v.Add(4, 4)
	v.Add(5, 5)
	v.Add(6, 6)

	t.Run("no values", func(t *testing.T) {
		result := rangeQuery(context.Background(), v, nil, nil, false, false)
		require.ElementsMatch(t, []uint32{}, result.ToArray())
	})
	t.Run("(1..", func(t *testing.T) {
		var from int32 = 1
		result := rangeQuery(context.Background(), v, &from, nil, false, false)
		require.ElementsMatch(t, []uint32{2, 3, 4, 5, 6}, result.ToArray())
	})
	t.Run("[1..", func(t *testing.T) {
		var from int32 = 1
		result := rangeQuery(context.Background(), v, &from, nil, true, false)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.ToArray())
	})
	t.Run("..3)", func(t *testing.T) {
		var to int32 = 3
		result := rangeQuery(context.Background(), v, nil, &to, false, false)
		require.ElementsMatch(t, []uint32{1, 2}, result.ToArray())
	})
	t.Run("..3]", func(t *testing.T) {
		var to int32 = 3
		result := rangeQuery(context.Background(), v, nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3}, result.ToArray())
	})
	t.Run("..6]", func(t *testing.T) {
		var to int32 = 6
		result := rangeQuery(context.Background(), v, nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.ToArray())
	})
	t.Run("..7]", func(t *testing.T) {
		var to int32 = 6
		result := rangeQuery(context.Background(), v, nil, &to, false, true)
		require.ElementsMatch(t, []uint32{1, 2, 3, 4, 5, 6}, result.ToArray())
	})
	t.Run("(1..4]", func(t *testing.T) {
		var from int32 = 1
		var to int32 = 4
		result := rangeQuery(context.Background(), v, &from, &to, false, true)
		require.ElementsMatch(t, []uint32{2, 3, 4}, result.ToArray())
	})
	t.Run("[100, 1000]", func(t *testing.T) {
		var from int32 = 100
		var to int32 = 1000
		result := rangeQuery(context.Background(), v, &from, &to, false, true)
		require.ElementsMatch(t, []uint32{}, result.ToArray())
	})
}
