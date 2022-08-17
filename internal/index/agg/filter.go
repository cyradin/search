package agg

import (
	"context"
	"encoding/json"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/index/query"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
)

var _ Agg = (*FilterAgg)(nil)

type FilterResult struct {
	DocCount int                    `json:"docCount"`
	Aggs     map[string]interface{} `json:"aggs,omitempty"`
}

type FilterAgg struct {
	Filter query.Query `json:"filter"`
	Aggs   Aggs        `json:"aggs"`
}

func (a *FilterAgg) UnmarshalJSON(data []byte) error {
	qdata := new(struct {
		Filter json.RawMessage `json:"filter"`
		Aggs   Aggs            `json:"aggs"`
	})
	err := jsoniter.Unmarshal(data, qdata)
	if err != nil {
		return err
	}

	q, err := query.Build(query.QueryRequest(qdata.Filter))
	if err != nil {
		return err
	}
	a.Filter = q
	a.Aggs = qdata.Aggs

	return nil
}

func (a *FilterAgg) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Filter, validation.Required, validation.NotNil),
	)
}

func (a *FilterAgg) Exec(ctx context.Context, fields Fields, docs *roaring.Bitmap) (interface{}, error) {
	res, err := a.Filter.Exec(ctx, query.Fields(fields))
	if err != nil {
		return nil, err
	}

	resDocs := res.Docs()
	resDocs.And(docs)

	result := FilterResult{
		DocCount: int(resDocs.GetCardinality()),
	}
	if len(a.Aggs) > 0 {
		result.Aggs = make(map[string]interface{}, len(a.Aggs))
		for key, subAgg := range a.Aggs {
			subAggResult, err := subAgg.Exec(ctx, fields, resDocs)
			if err != nil {
				return nil, err
			}
			result.Aggs[key] = subAggResult
		}
	}

	return result, nil
}
