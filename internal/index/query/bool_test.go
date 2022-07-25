package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_boolQuery(t *testing.T) {
	t.Run("newBoolQuery", func(t *testing.T) {
		t.Run("must not return error if request is an empty object", func(t *testing.T) {
			query := "{}"
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
		t.Run("must return error if request contains extra keys", func(t *testing.T) {
			query := `{
				"should": [],
				"field": []
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must return error if request field is not an array", func(t *testing.T) {
			query := `{
				"should": {
					"query": "hello"
				}
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request field is an empty array", func(t *testing.T) {
			query := `{
				"should": []
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, q)
		})
		t.Run("must return error if request field contains invalid subquery", func(t *testing.T) {
			query := `{
				"should": [
					{
						"term": 1
					}
				]
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
		t.Run("must not return error if request is a valid query", func(t *testing.T) {
			query := `{
				"should": [
					{
						"term": {
							"query": "value"
						}
					}
				],
				"must": [
					{
						"bool": {
							"should": [
								{
									"term": {
										"query": "value"
									}
								}
							]
						}
					}
				],
				"filter": [
					{
						"bool": {}
					}
				]
			}`
			req, err := decodeQuery(query)
			require.NoError(t, err)

			q, err := newBoolQuery(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, q)
		})
	})

	t.Run("exec", func(t *testing.T) {
		f1 := field.NewBool()
		f1.Add(1, true)
		f1.Add(2, false)

		f2 := field.NewAll()
		f2.Add(1, true)
		f2.Add(2, false)
		f2.Add(3, true)

		ctx := withFields(context.Background(), map[string]field.Field{"field": f1, field.AllField: f2})

		t.Run("must return all documents for empty query", func(t *testing.T) {
			query, err := decodeQuery(`{}`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
		})
		t.Run("must return documents for should query if documents match at least one query", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{1, 2}, result.Docs().ToArray())
		})
		t.Run("must return no documents for must query if no documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
		})
		t.Run("must return documents for must query if some documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": true
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
		})
		t.Run("must return no documents for must+should query if no documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				],
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
		})
		t.Run("must return no documents for filter query if no documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"filter": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
		})
		t.Run("must return documents for filter query if some documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"filter": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": true
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
		})
		t.Run("must return no documents for filter+should query if no documents match all queries", func(t *testing.T) {
			query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				],
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			bq, err := newBoolQuery(ctx, query)
			require.NoError(t, err)
			result, err := bq.exec(ctx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
		})
	})
}
