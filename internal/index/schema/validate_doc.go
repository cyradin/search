package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

func ValidateDoc(s Schema, source map[string]interface{}) error {
	rules := buildRules(s, source)

	return rules.Validate(source)
}

func buildRules(s Schema, source map[string]interface{}) validation.MapRule {
	var rules []*validation.KeyRules

	for name, f := range s.Fields {
		var keyRules []validation.Rule
		if f.Required {
			keyRules = append(keyRules, validation.Required)
		} else if _, ok := source[name]; !ok {
			continue
		}

		switch f.Type {
		case TypeBool:
			keyRules = append(keyRules, validation.By(validateBool()))
		case TypeKeyword:
			keyRules = append(keyRules, validation.By(validateKeyword()))
		case TypeText:
			keyRules = append(keyRules, validation.By(validateText()))
		case TypeByte:
			keyRules = append(keyRules, validation.By(validateInt(math.MinInt8, math.MaxInt8)))
		case TypeShort:
			keyRules = append(keyRules, validation.By(validateInt(math.MinInt16, math.MaxInt16)))
		case TypeInteger:
			keyRules = append(keyRules, validation.By(validateInt(math.MinInt32, math.MaxInt32)))
		case TypeLong:
			keyRules = append(keyRules, validation.By(validateInt(math.MinInt64, math.MaxInt64)))
		case TypeUnsignedLong:
			keyRules = append(keyRules, validation.By(validateUint(0, math.MaxUint64)))
		case TypeFloat:
			keyRules = append(keyRules, validation.By(validateFloat(-1*math.MaxFloat32, math.MaxFloat32)))
		case TypeDouble:
			keyRules = append(keyRules, validation.By(validateFloat(-1*math.MaxFloat64, math.MaxFloat64)))
		}

		rules = append(rules, validation.Key(name, keyRules...))
	}

	return validation.Map(rules...)
}

func validateBool() validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		_, ok := v.(bool)
		if !ok {
			return fmt.Errorf("required bool, got %#v", v)
		}

		return nil
	}
}

func validateKeyword() validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		_, ok := v.(string)
		if !ok {
			return fmt.Errorf("required string, got %#v", v)
		}

		return nil
	}
}

func validateText() validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		_, ok := v.(string)
		if !ok {
			return fmt.Errorf("required string, got %#v", v)
		}

		return nil
	}
}

func validateInt(min int64, max int64) validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		vv, ok := v.(json.Number)
		if !ok {
			return fmt.Errorf("required int, got %#v", v)
		}
		vvv, err := strconv.ParseInt(vv.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as int", vv.String())
		}

		if vvv > max {
			return fmt.Errorf("must be <= than %d", max)
		}

		if vvv < min {
			return fmt.Errorf("must be >= than %d", min)
		}

		return nil
	}
}

func validateUint(min uint64, max uint64) validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		vv, ok := v.(json.Number)
		if !ok {
			return fmt.Errorf("required uint, got %#v", v)
		}
		vvv, err := strconv.ParseUint(vv.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as uint", vv.String())
		}

		if vvv > max {
			return fmt.Errorf("must be <= %d", max)
		}

		if vvv < min {
			return fmt.Errorf("must be >= than %d", min)
		}

		return nil
	}
}

func validateFloat(min float64, max float64) validation.RuleFunc {
	return func(v interface{}) error {
		if v == nil {
			return nil
		}
		vv, ok := v.(json.Number)
		if !ok {
			return fmt.Errorf("required float, got %#v", v)
		}
		vvv, err := strconv.ParseFloat(vv.String(), 64)
		if err != nil {
			return fmt.Errorf("cannot parse %q as float", vv.String())
		}

		if vvv > max {
			return fmt.Errorf("must be <= than %f", max)
		}

		if vvv < min {
			return fmt.Errorf("must be >= than %f", min)
		}

		return nil
	}
}
