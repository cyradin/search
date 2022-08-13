package agg

import (
	"testing"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/stretchr/testify/require"
)

func Test_RangeAgg_Validate(t *testing.T) {
	t.Run("must return error if request is an empty object", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' not defined", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' are empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' 'key' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"from": 10,
					"to": 50
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must return error if request 'ranges' 'from' and 'to' are empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key"
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.Error(t, err)
	})

	t.Run("must not return error if request 'ranges' 'from' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key",
					"to": 50
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.NoError(t, err)
	})

	t.Run("must not return error if request 'ranges' 'to' is empty", func(t *testing.T) {
		v := new(RangeAgg)
		mustUnmarshal(t, `{
			"ranges": [
				{
					"key": "key",
					"from": 10
				}
			],
			"field": "field"
		}`, v)

		err := validation.Validate(v)
		require.NoError(t, err)
	})
}
