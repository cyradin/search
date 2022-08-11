package valid

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_JsonNumber(t *testing.T) {
	t.Run("must return error if value is not a json.Number", func(t *testing.T) {
		cb := JsonNumber()
		err := cb(context.Background(), "str")
		require.Error(t, err)
	})
	t.Run("must not return error if value is a json.Number", func(t *testing.T) {
		cb := JsonNumber()
		err := cb(context.Background(), json.Number("123"))
		require.NoError(t, err)
	})
}

func Test_JsonNumberInt(t *testing.T) {
	t.Run("must return error if value is not an json.Number integer", func(t *testing.T) {
		cb := JsonNumberInt()
		err := cb(context.Background(), "str")
		require.Error(t, err)
	})

	t.Run("must return error if value is not an integer", func(t *testing.T) {
		cb := JsonNumberInt()
		err := cb(context.Background(), json.Number("123.1"))
		require.Error(t, err)
	})

	t.Run("must not return error if value is a json.Number int", func(t *testing.T) {
		cb := JsonNumberInt()
		err := cb(context.Background(), json.Number("123"))
		require.NoError(t, err)
	})
}

func Test_JsonNumberFloat(t *testing.T) {
	t.Run("must return error if value is not an json.Number integer", func(t *testing.T) {
		cb := JsonNumberFloat()
		err := cb(context.Background(), "str")
		require.Error(t, err)
	})

	t.Run("must return error if value is not a floating point value", func(t *testing.T) {
		cb := JsonNumberFloat()
		err := cb(context.Background(), json.Number("qwe"))
		require.Error(t, err)
	})

	t.Run("must not return error if value is a json.Number float", func(t *testing.T) {
		cb := JsonNumberFloat()
		err := cb(context.Background(), json.Number("123"))
		require.NoError(t, err)

		err = cb(context.Background(), json.Number("123.1"))
		require.NoError(t, err)
	})
}

func Test_JsonNumberIntMin(t *testing.T) {
	t.Run("must return error if value is not an json.Number integer", func(t *testing.T) {
		cb := JsonNumberIntMin(5)
		err := cb(context.Background(), "str")
		require.Error(t, err)
	})

	t.Run("must return error if value is not an integer", func(t *testing.T) {
		cb := JsonNumberIntMin(5)
		err := cb(context.Background(), json.Number("123.1"))
		require.Error(t, err)
	})

	t.Run("must return error if value < min", func(t *testing.T) {
		cb := JsonNumberIntMin(5)
		err := cb(context.Background(), json.Number("1"))
		require.Error(t, err)
	})

	t.Run("must not return error if value >= min", func(t *testing.T) {
		cb := JsonNumberIntMin(5)
		err := cb(context.Background(), json.Number("5"))
		require.NoError(t, err)
	})
}
