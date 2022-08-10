package agg

import (
	"context"
	"fmt"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/cyradin/search/internal/errs"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func multipleAggDefinitionsErr(ctx context.Context, aggs ...string) validation.Error {
	return validation.NewError("validation_aggs_multiple_definitions_not_allowed", fmt.Sprintf("multiple agg definitions not allowed: %s", strings.Join(aggs, ", "))).
		SetParams(errs.Params(errs.Path(ctx)))
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
		ctx := errs.WithPath(ctx, errs.Path(ctx), key)

		aType, err := getAggType(req)
		if err != nil {
			panic(err) // must not be executed because of validation made earlier
		}

		var (
			r internalAgg
		)
		switch aggType(aType) {
		case aggTerms:
			r, err = newTermAgg(ctx, req)
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
	for k := range req {
		ctx = errs.WithPath(ctx, k)
		rules = append(
			rules,
			validation.Key(
				k,
				validation.Required.ErrorObject(errs.Required(ctx)),
				validation.WithContext(func(ctx context.Context, value interface{}) error {
					aggs, ok := value.(map[string]interface{})
					if !ok {
						return errs.ObjectRequired(ctx, k)
					}

					var aggKey string
					for k, v := range aggs {
						if k == "aggs" {
							continue
						}

						if _, ok := aggTypes[aggType(k)]; !ok {
							return errs.UnknownValue(ctx, k)
						}

						if aggKey != "" {
							return multipleAggDefinitionsErr(ctx, aggKey, k)
						}

						aggKey = k
						_, ok := v.(map[string]interface{})
						if !ok {
							return errs.ObjectRequired(ctx, k)
						}
					}

					return nil
				}),
			),
		)
	}

	return validation.ValidateWithContext(ctx, req,
		validation.Required.ErrorObject(errs.Required(ctx)),
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
		if k == "aggs" {
			continue
		}
		return k, nil
	}

	return "", fmt.Errorf("failed to determine agg type")
}
