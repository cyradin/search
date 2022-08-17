package agg

import (
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
)

type AggType struct {
	Type string `json:"type"`
}

type Aggs map[string]Agg

func (s *Aggs) UnmarshalJSON(data []byte) error {
	d := make(AggsRequest)
	err := jsoniter.Unmarshal(data, &d)
	if err != nil {
		return err
	}

	res, err := build(d)
	if err != nil {
		return err
	}

	*s = res
	return nil
}

func build(req AggsRequest) (Aggs, error) {
	if len(req) == 0 {
		return make(Aggs), nil
	}

	result := make(map[string]Agg, len(req))
	for key, value := range req {
		var (
			agg Agg
			err error
		)

		aggType := new(AggType)
		err = jsoniter.Unmarshal(value, aggType)
		if err != nil {
			return nil, err
		}

		switch aggType.Type {
		case "terms":
			agg = new(TermsAgg)
		case "range":
			agg = new(RangeAgg)
		case "filter":
			agg = new(FilterAgg)
		case "min":
			agg = new(MinAgg)
		default:
			return nil, fmt.Errorf("unknown agg type %q", aggType.Type)
		}

		err = jsoniter.Unmarshal(value, agg)
		if err != nil {
			return nil, err
		}

		err = validation.Validate(agg)
		if err != nil {
			return nil, err
		}

		result[key] = agg
	}

	return result, nil
}
