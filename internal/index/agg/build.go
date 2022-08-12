package agg

import (
	"context"
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	"github.com/cyradin/search/internal/valid"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func multipleAggDefinitionsErr(ctx context.Context, aggs ...string) validation.Error {
	return validation.NewError("validation_aggs_multiple_definitions_not_allowed", fmt.Sprintf("multiple agg definitions not allowed: %s", strings.Join(aggs, ", "))).
		SetParams(valid.ErrParams(valid.Path(ctx)))
}

type internalAgg interface {
	exec(ctx context.Context, docs *roaring.Bitmap) (interface{}, error)
}

type aggType string

const (
	aggTerms aggType = "terms"
	aggRange aggType = "range"
)

var aggTypes = map[aggType]struct{}{
	aggTerms: {},
	aggRange: {},
}

func build(ctx context.Context, req Aggs) (map[string]internalAgg, error) {
	if req == nil || len(req) == 0 {
		return make(map[string]internalAgg), nil
	}
	err := validateAggs(ctx, req)
	if err != nil {
		return nil, err
	}

	result := make(map[string]internalAgg, len(req))
	for key, value := range req {
		req := value.(map[string]interface{})
		ctx := valid.WithPath(ctx, valid.Path(ctx), key)

		aType, err := getAggType(req)
		if err != nil {
			panic(err) // must not be executed because of validation made earlier
		}
		agg, subAgg := getAggData(req)

		var r internalAgg
		switch aggType(aType) {
		case aggTerms:
			r, err = newTermsAgg(ctx, agg, subAgg)
		default:
			panic(errs.Errorf("unknown agg type %q", key)) // must not be executed because of validation made earlier
		}

		if err != nil {
			return nil, err
		}

		result[key] = r
	}

	return result, nil
}

func validateAggs(ctx context.Context, req Aggs) error {
	rules := make([]*validation.KeyRules, 0, len(req))
	for key := range req {
		ctx = valid.WithPath(ctx, key)
		rules = append(
			rules,
			validation.Key(
				key,
				validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
				validation.WithContext(func(ctx context.Context, value interface{}) error {
					aggs, ok := value.(map[string]interface{})
					if !ok {
						return valid.NewErrObjectRequired(ctx, key)
					}

					var aggKey string
					for k, v := range aggs {
						_, ok := v.(map[string]interface{})
						if !ok {
							return valid.NewErrObjectRequired(ctx, k)
						}
						if k == AggsKey {
							continue
						}

						if _, ok := aggTypes[aggType(k)]; !ok {
							return valid.NewErrUnknownValue(ctx, k)
						}

						if aggKey != "" {
							return multipleAggDefinitionsErr(ctx, aggKey, k)
						}

						aggKey = k
					}

					return nil
				}),
			),
		)
	}

	return validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(valid.NewErrRequired(ctx)),
		validation.Map(rules...).AllowExtraKeys(),
	)
}

func firstVal(m map[string]interface{}) (string, interface{}) {
	for k, v := range m {
		return k, v
	}

	return "", nil
}

func getAggType(req Aggs) (string, error) {
	for k := range req {
		if k == AggsKey {
			continue
		}
		return k, nil
	}

	return "", errs.Errorf("failed to determine agg type")
}

func getAggData(req Aggs) (map[string]interface{}, map[string]interface{}) {
	var agg map[string]interface{}
	var subAggs map[string]interface{}
	for k, v := range req {
		if k == AggsKey {
			subAggs = v.(map[string]interface{})
			continue
		}
		agg = v.(map[string]interface{})
	}

	return agg, subAggs
}
