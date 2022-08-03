package agg

import (
	"context"

	"github.com/RoaringBitmap/roaring"
)

type rangeAggRange[T NumericConstraint] struct {
	Key  string
	From *T
	To   *T
}

type RangeAggBucket struct {
	Key      string      `json:"key"`
	From     interface{} `json:"from"`
	To       interface{} `json:"to"`
	DocCount int         `json:"docCount"`
}

type RangeAggResult struct {
	Buckets []RangeAggBucket
}

func rangeAgg[T NumericConstraint](ctx context.Context, docs *roaring.Bitmap, values *docValues[T], ranges []rangeAggRange[T]) RangeAggResult {
	result := RangeAggResult{
		Buckets: make([]RangeAggBucket, 0, len(ranges)),
	}

	for _, r := range ranges {
		if docs == nil || docs.IsEmpty() || (r.From == nil && r.To == nil) {
			result.Buckets = append(result.Buckets, RangeAggBucket{
				From:     any(r.From),
				To:       any(r.To),
				Key:      r.Key,
				DocCount: 0,
			})
			continue
		}

		d := rangeQuery(ctx, values, r.From, r.To, true, true)
		d.And(docs)

		result.Buckets = append(result.Buckets, RangeAggBucket{
			From:     r.From,
			To:       r.To,
			Key:      r.Key,
			DocCount: int(d.GetCardinality()),
		})
	}

	return result
}

func rangeQuery[T NumericConstraint](ctx context.Context, values *docValues[T], from *T, to *T, incFrom, incTo bool) *roaring.Bitmap {
	if from == nil && to == nil {
		return roaring.New()
	}

	fromIndex := 0
	toIndex := values.Cardinality() - 1
	if from != nil {
		if incFrom {
			fromIndex = values.FindGte(*from)
		} else {
			fromIndex = values.FindGt(*from)
		}
	}

	if to != nil {
		if incTo {
			toIndex = values.FindLte(*to)
		} else {
			toIndex = values.FindLt(*to)
		}
	}

	if fromIndex == values.Cardinality() || toIndex == values.Cardinality() || fromIndex > toIndex {
		return roaring.New()
	}

	bm := make([]*roaring.Bitmap, 0, toIndex-fromIndex+1)
	for i := fromIndex; i <= toIndex; i++ {
		v := values.DocsByValue(values.ValueByIndex(i))
		bm = append(bm, v)
	}

	return roaring.FastOr(bm...)
}
