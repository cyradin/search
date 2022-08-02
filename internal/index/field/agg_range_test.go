package field

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_rangeAgg(t *testing.T) {
	v := newDocValues[int32]()
	v.Add(1, 1)
	v.Add(2, 2)
	v.Add(3, 3)
	v.Add(4, 4)
	v.Add(5, 5)
	v.Add(6, 6)

	var from int32 = 1
	var to int32 = 4
	ranges := []rangeAggRange[int32]{
		{
			Key: "empty",
		},
		{
			Key:  "1..4",
			From: &from,
			To:   &to,
		},
	}

	checkResult := func(t *testing.T, expected RangeAggResult, result RangeAggResult) {
		require.Len(t, result.Buckets, len(expected.Buckets))
		for i, v := range result.Buckets {
			assert.EqualValues(t, expected.Buckets[i].Key, v.Key)
			if expected.Buckets[i].From == nil {
				assert.Nil(t, v.From)
			} else {
				assert.EqualValues(t, expected.Buckets[i].From, v.From)
			}
			if expected.Buckets[i].To == nil {
				assert.Nil(t, v.To)
			} else {
				assert.EqualValues(t, expected.Buckets[i].To, v.To)
			}
			assert.EqualValues(t, expected.Buckets[i].DocCount, v.DocCount)
		}
	}

	t.Run("must return empty buckets if nil bitmap provided", func(t *testing.T) {
		result := rangeAgg(context.Background(), nil, v, ranges)
		expected := RangeAggResult{Buckets: []RangeAggBucket{
			{Key: "empty"},
			{Key: "1..4", From: &from, To: &to},
		}}
		checkResult(t, expected, result)
	})
	t.Run("must return empty buckets if empty bitmap provided", func(t *testing.T) {
		result := rangeAgg(context.Background(), roaring.New(), v, ranges)
		expected := RangeAggResult{Buckets: []RangeAggBucket{
			{Key: "empty"},
			{Key: "1..4", From: &from, To: &to},
		}}
		checkResult(t, expected, result)
	})
	t.Run("must return correct buckets", func(t *testing.T) {
		bm := roaring.New()
		bm.Add(1)
		bm.Add(2)
		bm.Add(3)

		result := rangeAgg(context.Background(), bm, v, ranges)
		expected := RangeAggResult{Buckets: []RangeAggBucket{
			{Key: "empty"},
			{
				Key:      "1..4",
				From:     &from,
				To:       &to,
				DocCount: 3,
			},
		}}
		checkResult(t, expected, result)
	})
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
